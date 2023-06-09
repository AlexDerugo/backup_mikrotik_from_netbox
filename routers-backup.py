import os, git, pynetbox
from time import sleep
from scrapli import Scrapli
from scrapli.exceptions import ScrapliException
from paramiko import SSHClient, AutoAddPolicy
from scp import SCPClient
from datetime import datetime
from google.cloud import storage

date                        = datetime.now().strftime("%Y-%m-%d")
bucket_name                 = "bucket_for_backup"                       # имя bucket GCP, в который загружаем файлы
bucket_key                  = "/path/to/gcp-key.json"                   # key для подключения к bucket GCP
folder_project              = "/path/to/folder_with_backup"             # текущая папка с проектом

# netbox connection
NETBOX_URL                  = "http://netbox.url"
TOKEN                       = "token_netbox"
nb                          = pynetbox.api(url=NETBOX_URL, token=TOKEN)
tag_for_backup              = "mikrotik"

# параметры подключения к mikrotik
user_for_backup             = "user_in_mikrotik"
path_to_private_key         = "/path/to/.ssh/cert"      # ssh сертифкат для подключения
port_ssh                    = 333

# переменные имен файлов для бэкапа
name_export_file            = "export.rsc"
name_config_file            = "config.backup"

# подключение к netbox и получение словаря mikrot_name : mikrot_ip с помощью tag mikrotik. Если надо бэкапить конфигурацию микротика вешаем tag и проверяем, чтобы был primary ip
def get_mikrotik_from_netbox():
    list_mikrotiks                      = {}                                                                # создаем словарь в который будем добавлять имя и Ip микротика
    mikrotik_vm_from_netbox             = nb.virtualization.virtual_machines.filter(tag = tag_for_backup)   # получаем микротики VM с нужным tag
    for mikrotik in mikrotik_vm_from_netbox:
        list_mikrotiks[mikrotik.name]   = mikrotik.primary_ip                                               # добавляем в словарь пару {name : ip}
    mikrotik_device_from_netbox         = nb.dcim.devices.filter(tag = tag_for_backup)                      # получаем микротики Device с нужным tag
    for mikrotik in mikrotik_device_from_netbox:
        list_mikrotiks[mikrotik.name]   = mikrotik.primary_ip                                               # добавляем в словарь пару {name : ip}
    return list_mikrotiks                                                                                   # функция возвращает словарь с нужными микротиками
    
# подключаемся к микротикам и создаем локальные файлы бэкапов
def create_backup_files_inside_mikrotik(mikrotik):
    # команды для создания локальных файлов(просто export в файл и bin файл бэкапа)
    commands = [f"/export file={name_export_file}",
                f"/system backup save name={name_config_file} dont-encrypt=yes"]
    # подключаемся с помощью scrapli и выполняем каманды на микротиках. Асинхронный метод не со всех серверов получилось запустить
    try:
        with Scrapli(**mikrotik) as ssh:
            ssh.send_commands(commands)
    except ScrapliException as error:
        print(error, " in ", mikrotik["host"])

# скачиваем файлы с микротика и качаем их в бакет GCP
def download_files_from_mikrotik_and_push_to_gcp(ip, router):
    folder_router = os.path.join(folder_project, router)                                            # под каждый роутер свой репозитарий внутри проекта. имя такое же как имя роутера
    if os.path.exists(folder_router):                                                               # если репозитарий уже есть, то ничего не делаем
        None
    else:
        os.makedirs(folder_router)                                                                  # если репозитария нет, то создаем его
    path_bin_files      = os.path.join(folder_router, name_config_file)                             # путь к bin файлу в каждом репозитарии. имя файла из общей переменной, одинаковое для всех
    path_export_files   = os.path.join(folder_router, name_export_file)                             # путь к export файлу в каждом репозитарии. имя файла из общей переменной, одинаковое для всех
    try:
        # создаем ssh подключение с помощью paramiko
        with SSHClient() as ssh:
            ssh.set_missing_host_key_policy(AutoAddPolicy())
            ssh.connect(ip,
                        port                = port_ssh,
                        username            = user_for_backup,
                        look_for_keys       = False,
                        allow_agent         = False,
                        disabled_algorithms = dict(pubkeys=['rsa-sha2-256', 'rsa-sha2-512']),       # нужно из-за бага paramiko 2.11.0 при работе с open ssh файлами. должны пофиксить в новых релизах
                        key_filename        = path_to_private_key)
            # качаем файлы с пощью scp на основе ssh от paramiko
            with SCPClient(ssh.get_transport()) as scp:
                scp.get(name_config_file, path_bin_files)
                scp.get(name_export_file, path_export_files)
    except:
        print(f"{router} error with scp")
    sleep(5)                                                                # пауза на всякий случай, если будут проблемы с долгим скачиванием файлов
    # push to GCP bucket
    storage_client  = storage.Client.from_service_account_json(bucket_key ) # запускаем клиента GCP storage
    bucket          = storage_client.get_bucket(bucket_name)                # подключаемся к нашему бакету
    # создаем файл в бакете, имя с разделителями / , чтобы файлы лежали в репозитариях по датам и именам роутеров. копируем файл из локального репозитария, меняя имя на заданное
    try:
        blob_config_file            = bucket.blob(f"{router}/{date}/{router}-{name_config_file}")
        blob_config_file.upload_from_filename(path_bin_files)
        blob_export_file            = bucket.blob(f"{router}/{date}/{router}-{name_export_file}")
        blob_export_file.upload_from_filename(path_export_files)
    except:
        print(f"problem with upload in GCP for {router}")

# загружаем в Git изменения. Бэкапы только export, bin файлы исключены в .gitignore.
def push_git():
    try:
        with git.Repo.init(path=folder_project) as repo:        # инициируем репозитарий проекта
            repo.git.add('--all')                               # добавляем все файлы
            repo.index.commit(f"{date} commit from script")     # делаем commit
            origin = repo.remote(name="origin")                 # в проекте работаем только с master, не делаем отдельных бранчей
            origin.push()                                       # загружаем изменения в Git
    except:
        print("problem with git push")


if __name__ == "__main__":
    mikrotik_from_netbox = get_mikrotik_from_netbox()                           # получаем словарь с данными по микротикам
    for router in mikrotik_from_netbox.keys():                                  # запукаем функции по каждому роутеру отдельно
        ip          = str(mikrotik_from_netbox.get(router)).split("/")[0]       # получаем IP без маски. Т.к. в netbox IP с маской (IP/MASK), а для подключения нужен только IP, иначе будет ошибка
        mikrotik    = {                                                         # параметры для подключения
            "host"              : ip,
            "auth_username"     : user_for_backup,
            "auth_strict_key"   : False,
            "platform"          : "mikrotik_routeros",
            "auth_private_key"  : path_to_private_key,
            "port"              : port_ssh,
            #"timeout_socket"   : 10,
            #"timeout_transport": 15,
        }
        create_backup_files_inside_mikrotik(mikrotik)                           # подключаемся к микротику и создаем локальные файлы конфигурации
        download_files_from_mikrotik_and_push_to_gcp(ip, router)                # скачиваем файлы на локальный сервер и пушим их в бакет GCP
        sleep(5)                                                                # пауза на всякий случай, если возникнут задержки со скачиванием
    push_git()                                                                  # пушим изменения в Git