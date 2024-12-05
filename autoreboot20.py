import requests
import logging
import os
import yaml
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("node_reboot.log", mode='w', encoding='utf-8'),
        logging.StreamHandler()  # Вывод только сообщений уровня INFO и выше в терминал
    ]
)

REBOOT_INTERVAL_HOURS = 2.5

# Словарь для хранения статистики перезапусков по кластерам
reboot_stats = {}

def get_own_ip():
    try:
        response = requests.get("https://api.ipify.org?format=json")
        response.raise_for_status()
        ip = response.json().get("ip")
        logging.info(f"Текущий IP сервера: {ip}")
        return ip
    except requests.RequestException as e:
        logging.error(f"Ошибка при получении собственного IP-адреса: {e}")
        return None

def fetch_node_status(ip):
    url_template = "https://incentive-backend.oceanprotocol.com/nodes?page=1&size=100&search={ip}"
    url = url_template.format(ip=ip)
    logging.info(f"Проверка статуса нод на IP {ip}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        nodes = data.get("nodes", [])

        eligible_nodes = []
        for node in nodes:
            node_info = node.get("_source", {})
            if node_info.get("eligible", False):
                allowed_admins = node_info.get("allowedAdmins", [])
                eligible_nodes.append({
                    "ip": ip,
                    "node_id": node_info.get("id", "unknown"),
                    "allowed_admins": allowed_admins
                })
        total_nodes = len(nodes)
        non_eligible_nodes = total_nodes - len(eligible_nodes)
        logging.info(f"Всего нод: {total_nodes}, Eligible: {len(eligible_nodes)}, Non-eligible: {non_eligible_nodes}")
        return eligible_nodes

    except requests.RequestException as e:
        logging.error(f"Ошибка при запросе для IP {ip}: {e}")
    except Exception as e:
        logging.error(f"Неизвестная ошибка для IP {ip}: {e}")
    
    return []

def reboot_nodes_on_server(ocean_base_folder, eligible_nodes, rebooted_nodes):
    try:
        logging.info(f"Перезапуск нод на локальном сервере")

        all_nodes_started = True
        total_nodes = 0
        eligible_count = len(eligible_nodes)
        non_eligible_count = 0

        # Поиск всех нод внутри /root/ocean/ocean_<номер>
        for i in range(1, 31):
            node_folder = f"{ocean_base_folder}/ocean_{i}"
            docker_compose_path = f"{node_folder}/docker-compose.yml"
            
            # Проверка существования файла docker-compose.yml
            if not os.path.isfile(docker_compose_path):
                logging.warning(f"Файл docker-compose.yml не найден в папке {node_folder}")
                continue

            total_nodes += 1

            with open(docker_compose_path, 'r') as f:
                docker_compose_content = f.read()

            # Проверка содержимого docker-compose.yml, чтобы понять, что это нужная нода
            try:
                docker_compose = yaml.safe_load(docker_compose_content)
                if "services" in docker_compose and "ocean-node" in docker_compose["services"]:
                    service = docker_compose["services"]["ocean-node"]
                    environment = service.get("environment", {})
                    if environment:
                        allowed_admins_str = environment.get("ALLOWED_ADMINS", "[]")
                        allowed_admins = json.loads(allowed_admins_str)
                        
                        if node_is_eligible:
                            logging.info(f"Пропуск ноды в {node_folder} так как она является eligible")

                        # Перезагружаем ноду, если она не является eligible
                        if not node_is_eligible:
                            logging.info(f"Перезапуск ноды в {node_folder} так как она не является eligible")
                            non_eligible_count += 1
                            down_command = f"cd {node_folder} && docker compose down"
                            prune_command = "docker system prune -af"
                            up_command = f"cd {node_folder} && docker compose up -d"

                            logging.info(f"Остановка контейнера в {node_folder}")
                            os.system(down_command)

                            logging.info(f"Очистка системы на сервере")
                            os.system(prune_command)

                            logging.info(f"Запуск контейнера в {node_folder}")
                            os.system(up_command)
                            rebooted_nodes.append({"ip": "localhost", "node_id": node_folder})

                        # Обновленная проверка, что контейнер запущен
                        check_running_command = f"cd {node_folder} && docker compose ps"
                        running_status = os.popen(check_running_command).read().strip()
                        if 'Up' not in running_status:
                            all_nodes_started = False
                            logging.error(f"Контейнер в {node_folder} не запущен корректно")

            except (yaml.YAMLError, json.JSONDecodeError) as e:
                logging.error(f"Ошибка при разборе YAML или JSON файла docker-compose.yml в папке {node_folder}: {e}")

        # Обновление статистики перезапусков по кластеру
        cluster_name = "localhost"
        if cluster_name not in reboot_stats:
            reboot_stats[cluster_name] = {
                "last_reboot": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "total_nodes": 0,
                "eligible_nodes": 0,
                "non_eligible_nodes": 0
            }

        reboot_stats[cluster_name]["total_nodes"] += total_nodes
        reboot_stats[cluster_name]["eligible_nodes"] += eligible_count
        reboot_stats[cluster_name]["non_eligible_nodes"] += non_eligible_count

        if all_nodes_started:
            logging.info(f"Все ноды на сервере успешно запущены и работают корректно")
        else:
            logging.error(f"Некоторые ноды на сервере не были запущены корректно")

    except Exception as e:
        logging.error(f"Ошибка при перезагрузке ноды: {e}")

def main():
    while True:
        # Получаем текущий IP сервера
        ip = get_own_ip()
        if not ip:
            logging.error("Не удалось определить IP-адрес. Завершение работы.")
            return

        rebooted_nodes = []

        # Собираем информацию о нодах, которые являются eligible
        eligible_nodes = fetch_node_status(ip)

        # Перезагрузка нод на локальном сервере
        ocean_base_folder = "/root/ocean"
        reboot_nodes_on_server(ocean_base_folder, eligible_nodes, rebooted_nodes)

        # Отчет о перезагруженных нодах
        logging.info("\nОтчет о перезагруженных нодах:")
        if rebooted_nodes:
            for node in rebooted_nodes:
                logging.info(f"Нода с IP {node['ip']} и node_id {node['node_id']} была перезагружена.")
        else:
            logging.info("Не было перезагружено ни одной ноды.")

        # Отчет о статистике перезапусков по кластерам
        logging.info("\nСтатистика перезапусков по кластерам:")
        for cluster_name, stats in reboot_stats.items():
            logging.info(f"Кластер {cluster_name}: Последний перезапуск: {stats['last_reboot']}, Всего нод: {stats['total_nodes']}, Eligible: {stats['eligible_nodes']}, Non-eligible: {stats['non_eligible_nodes']}")

        # Сон на 2,5 часа с обратным отсчетом
        logging.info(f"\nСон на {REBOOT_INTERVAL_HOURS} часа перед следующей проверкой.")
        for remaining in range(int(REBOOT_INTERVAL_HOURS * 3600), 0, -60):
            logging.info(f"Осталось до следующей проверки: {remaining // 60} минут")
            time.sleep(60)

if __name__ == "__main__":
    main()
