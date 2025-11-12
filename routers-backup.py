import os
import logging
from time import sleep
from datetime import datetime
from git import Repo
from git.exc import GitCommandError, InvalidGitRepositoryError
import pynetbox
from pynetbox.core.query import RequestError
from scrapli import Scrapli
from scrapli.exceptions import ScrapliException
from paramiko import SSHClient, AutoAddPolicy
from paramiko import SSHException as ParamikoSSHException
from scp import SCPClient
from scp import SCPException
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError

# настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# конфигурируемые задержки
DELAY_AFTER_SCP = 5  # задержка после SCP операции
DELAY_BETWEEN_ROUTERS = 5  # задержка между обработкой роутеров

bucket_name                 = "bucket_for_backup"                       # имя bucket GCP, в который загружаем файлы
bucket_key                  = "/path/to/gcp-key.json"                   # key для подключения к bucket GCP
folder_project              = "/path/to/folder_with_backup"             # текущая папка с проектом

# netbox connection
NETBOX_URL                  = "http://netbox.url"
TOKEN                       = "token_netbox"
tag_for_backup              = "mikrotik"
nb                          = None  # будет инициализирован в функции с обработкой ошибок

# параметры подключения к mikrotik
user_for_backup             = "user_in_mikrotik"
path_to_private_key         = "/path/to/.ssh/cert"      # ssh сертифкат для подключения
port_ssh                    = 333

# переменные имен файлов для бэкапа
name_export_file            = "export.rsc"
name_config_file            = "config.backup"

# инициализация подключения к Netbox с обработкой ошибок
def init_netbox():
    global nb
    try:
        nb = pynetbox.api(url=NETBOX_URL, token=TOKEN)
        # проверяем подключение, делая тестовый запрос
        nb.status()
        logger.info(f"Successfully connected to Netbox at {NETBOX_URL}")
        return True
    except (RequestError, Exception) as error:
        logger.error(f"Failed to connect to Netbox: {error}")
        return False

# обработка одного типа устройств (VM или Device)
def _process_mikrotik_list(mikrotik_list, list_mikrotiks):
    for mikrotik in mikrotik_list:
        if mikrotik.primary_ip:
            if mikrotik.name in list_mikrotiks:
                logger.warning(f"Duplicate router name '{mikrotik.name}' found, overwriting previous entry")
            list_mikrotiks[mikrotik.name] = mikrotik.primary_ip
        else:
            logger.warning(f"Router '{mikrotik.name}' has no primary IP, skipping")

# подключение к netbox и получение словаря mikrot_name : mikrot_ip с помощью tag mikrotik. Если надо бэкапить конфигурацию микротика вешаем tag и проверяем, чтобы был primary ip
def get_mikrotik_from_netbox():
    if not nb:
        logger.error("Netbox connection not initialized")
        return {}
    list_mikrotiks = {}  # создаем словарь в который будем добавлять имя и Ip микротика
    try:
        mikrotik_vm_from_netbox = nb.virtualization.virtual_machines.filter(tag=tag_for_backup)  # получаем микротики VM с нужным tag
        _process_mikrotik_list(mikrotik_vm_from_netbox, list_mikrotiks)
        mikrotik_device_from_netbox = nb.dcim.devices.filter(tag=tag_for_backup)  # получаем микротики Device с нужным tag
        _process_mikrotik_list(mikrotik_device_from_netbox, list_mikrotiks)
        logger.info(f"Found {len(list_mikrotiks)} routers for backup")
        return list_mikrotiks  # функция возвращает словарь с нужными микротиками
    except (RequestError, Exception) as error:
        logger.error(f"Error fetching routers from Netbox: {error}")
        return {}
    
# подключаемся к микротикам и создаем локальные файлы бэкапов
def create_backup_files_inside_mikrotik(mikrotik):
    # команды для создания локальных файлов(просто export в файл и bin файл бэкапа)
    commands = [f"/export file={name_export_file}",
                f"/system backup save name={name_config_file} dont-encrypt=yes"]
    # подключаемся с помощью scrapli и выполняем каманды на микротиках. Асинхронный метод не со всех серверов получилось запустить
    host = mikrotik.get("host", "unknown")
    try:
        with Scrapli(**mikrotik) as ssh:
            responses = ssh.send_commands(commands)
            failed_commands = []
            for response in responses:
                if response.failed:
                    failed_commands.append(response.result)
                    logger.error(f"Command failed on {host}: {response.result}")
            if not failed_commands:
                logger.info(f"Successfully created backup files on {host}")
                return True
            return False
    except ScrapliException as error:
        logger.error(f"Scrapli error on {host}: {error}")
        return False

# скачиваем файлы с микротика
def download_files_from_mikrotik(ip, router):
    folder_router = os.path.join(folder_project, router)  # под каждый роутер свой репозитарий внутри проекта. имя такое же как имя роутера
    os.makedirs(folder_router, exist_ok=True)  # создаем директорию если её нет
    path_bin_files = os.path.join(folder_router, name_config_file)  # путь к bin файлу в каждом репозитарии. имя файла из общей переменной, одинаковое для всех
    path_export_files = os.path.join(folder_router, name_export_file)  # путь к export файлу в каждом репозитарии. имя файла из общей переменной, одинаковое для всех
    try:
        # создаем ssh подключение с помощью paramiko
        with SSHClient() as ssh:
            ssh.set_missing_host_key_policy(AutoAddPolicy())
            ssh.connect(ip,
                        port=port_ssh,
                        username=user_for_backup,
                        look_for_keys=False,
                        allow_agent=False,
                        disabled_algorithms=dict(pubkeys=['rsa-sha2-256', 'rsa-sha2-512']),  # нужно из-за бага paramiko 2.11.0 при работе с open ssh файлами. должны пофиксить в новых релизах
                        key_filename=path_to_private_key)
            # качаем файлы с пощью scp на основе ssh от paramiko
            with SCPClient(ssh.get_transport()) as scp:
                scp.get(name_config_file, path_bin_files)
                scp.get(name_export_file, path_export_files)
        logger.info(f"Successfully downloaded files from {router} ({ip})")
        sleep(DELAY_AFTER_SCP)  # пауза на всякий случай, если будут проблемы с долгим скачиванием файлов
        return path_bin_files, path_export_files
    except (ParamikoSSHException, SCPException, OSError) as error:
        logger.error(f"{router} error with scp: {error}")
        return None, None

# проверка размера файла (не пустой)
def _validate_file_size(file_path, min_size=100):
    if not os.path.exists(file_path):
        return False
    size = os.path.getsize(file_path)
    if size < min_size:
        logger.warning(f"File {file_path} is too small ({size} bytes), may be corrupted")
        return False
    return True

# загружаем файлы в бакет GCP
def push_to_gcp(router, path_bin_files, path_export_files, bucket, date):
    if not path_bin_files or not path_export_files:
        logger.warning(f"Skipping GCP upload for {router} due to SCP failure")
        return False
    # проверяем существование и размер файлов
    if not _validate_file_size(path_bin_files):
        logger.error(f"File {path_bin_files} does not exist or is invalid, skipping GCP upload")
        return False
    if not _validate_file_size(path_export_files):
        logger.error(f"File {path_export_files} does not exist or is invalid, skipping GCP upload")
        return False
    # создаем файл в бакете, имя с разделителями / , чтобы файлы лежали в репозитариях по датам и именам роутеров. копируем файл из локального репозитария, меняя имя на заданное
    try:
        blob_config_file = bucket.blob(f"{router}/{date}/{router}-{name_config_file}")
        blob_config_file.upload_from_filename(path_bin_files)
        blob_export_file = bucket.blob(f"{router}/{date}/{router}-{name_export_file}")
        blob_export_file.upload_from_filename(path_export_files)
        logger.info(f"Successfully uploaded files to GCP for {router}")
        return True
    except GoogleCloudError as error:
        logger.error(f"Problem with upload in GCP for {router}: {error}")
        return False

# загружаем в Git изменения. Бэкапы только export, bin файлы исключены в .gitignore.
def push_git(date):
    try:
        repo = Repo(path=folder_project)  # открываем существующий репозитарий проекта
        if repo.is_dirty() or repo.untracked_files:  # проверяем наличие изменений
            repo.git.add('--all')  # добавляем все файлы
            repo.index.commit(f"{date} commit from script")  # делаем commit
            origin = repo.remote(name="origin")  # в проекте работаем только с master, не делаем отдельных бранчей
            origin.push()  # загружаем изменения в Git
            logger.info("Successfully pushed changes to Git")
        else:
            logger.info("No changes to commit")
    except (GitCommandError, InvalidGitRepositoryError) as error:
        logger.error(f"Problem with git push: {error}")


# валидация конфигурации при старте
def validate_config():
    errors = []
    if not os.path.exists(bucket_key):
        errors.append(f"GCP key file not found: {bucket_key}")
    if not os.path.exists(path_to_private_key):
        errors.append(f"SSH private key not found: {path_to_private_key}")
    if not os.path.exists(folder_project):
        try:
            os.makedirs(folder_project, exist_ok=True)
            logger.info(f"Created project folder: {folder_project}")
        except OSError as error:
            errors.append(f"Cannot create project folder {folder_project}: {error}")
    if errors:
        for error in errors:
            logger.error(error)
        return False
    return True

if __name__ == "__main__":
    date = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Starting backup process for {date}")
    
    # валидация конфигурации
    if not validate_config():
        logger.error("Configuration validation failed, exiting")
        exit(1)
    
    # инициализация Netbox
    if not init_netbox():
        logger.error("Failed to initialize Netbox connection, exiting")
        exit(1)
    
    # получаем словарь с данными по микротикам
    mikrotik_from_netbox = get_mikrotik_from_netbox()
    if not mikrotik_from_netbox:
        logger.warning("No routers found for backup")
        exit(0)
    
    # создаем storage_client один раз для оптимизации
    try:
        storage_client = storage.Client.from_service_account_json(bucket_key)  # запускаем клиента GCP storage
        bucket = storage_client.get_bucket(bucket_name)  # подключаемся к нашему бакету
        logger.info(f"Successfully connected to GCP bucket: {bucket_name}")
    except (GoogleCloudError, OSError) as error:
        logger.error(f"Failed to connect to GCP bucket: {error}")
        exit(1)
    except Exception as error:
        logger.error(f"Unexpected error connecting to GCP: {error}")
        exit(1)
    
    # обрабатываем каждый роутер
    success_count = 0
    fail_count = 0
    for router in mikrotik_from_netbox.keys():
        logger.info(f"Processing router: {router}")
        primary_ip = mikrotik_from_netbox.get(router)
        if not primary_ip:
            logger.warning(f"Skipping {router}: no primary IP")
            fail_count += 1
            continue
        ip = str(primary_ip).split("/")[0]  # получаем IP без маски. Т.к. в netbox IP с маской (IP/MASK), а для подключения нужен только IP, иначе будет ошибка
        mikrotik = {  # параметры для подключения
            "host": ip,
            "auth_username": user_for_backup,
            "auth_strict_key": False,
            "platform": "mikrotik_routeros",
            "auth_private_key": path_to_private_key,
            "port": port_ssh,
            #"timeout_socket": 10,
            #"timeout_transport": 15,
        }
        # подключаемся к микротику и создаем локальные файлы конфигурации
        if create_backup_files_inside_mikrotik(mikrotik):
            # скачиваем файлы на локальный сервер
            path_bin_files, path_export_files = download_files_from_mikrotik(ip, router)
            # пушим их в бакет GCP
            if push_to_gcp(router, path_bin_files, path_export_files, bucket, date):
                success_count += 1
            else:
                fail_count += 1
        else:
            fail_count += 1
        sleep(DELAY_BETWEEN_ROUTERS)  # пауза на всякий случай, если возникнут задержки со скачиванием
    
    logger.info(f"Backup completed. Success: {success_count}, Failed: {fail_count}")
    push_git(date)  # пушим изменения в Git