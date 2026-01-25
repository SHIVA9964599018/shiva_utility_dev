from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions
from time import sleep
from threading import Thread
import math


#   Update this path to where you saved msedgedriver.exe
EDGE_DRIVER_PATH = r"C:\Users\shivac\Downloads\edgedriver_win64\msedgedriver.exe"


def add_jobs_to_cart(jobs, mapping_inputs, driver_index):
    options = EdgeOptions()
    options.add_experimental_option("detach", True)  # Keeps Edge browser open after script ends

    # Use manually downloaded driver
    driver = webdriver.Edge(service=EdgeService(EDGE_DRIVER_PATH), options=options)
    driver.get("https://ctmweb-prd1-01.cisco.com:8443/mapping_rules/folder_promotion")

    input(f"Driver {driver_index} is ready. Authenticate the webpage and then press Enter to start adding jobs to the cart...")

    Advanced_promotion = driver.find_element(By.LINK_TEXT, "Advanced Promotion")
    Advanced_promotion.click()
    sleep(4)
    Folder_promotion_request = driver.find_element(By.LINK_TEXT, "Folder Promotion Request")
    Folder_promotion_request.click()
    sleep(4)
    New_button = driver.find_element(By.XPATH, "//span[@class='icon-text' and text()='New']")
    New_button.click()
    sleep(4)
    Ok_button = driver.find_element(By.XPATH, "//button[@type='button' and @class='btn btn-primary bootbox-accept' and text()='OK']")
    Ok_button.click()
    sleep(4)

    from_env = driver.find_element(By.XPATH, f"//table[@id='source_env_table']//tr[td[text()='{mapping_inputs[0].upper()}']]//input[@type='radio']")
    from_env.click()
    sleep(4)

    Next_button = driver.find_element(By.XPATH, "//span[@class='icon-text' and text()='Next']")
    Next_button.click()
    sleep(10)

    for item in jobs:
        search_bar = driver.find_element("id", "folder")
        search_bar.clear()
        search_bar.send_keys(item)
        search_button = driver.find_element(By.XPATH, "//i[@class='fa fa-search']")
        search_button.click()
        sleep(10)

        result_items = driver.find_elements(By.XPATH, "//div[@id='data']//li[@role='treeitem']")
        if not result_items:
            continue
        
        for result in result_items:
            anchor = result.find_element(By.XPATH, ".//a[@class='jstree-anchor']")
            item_name = anchor.text.strip().split(":", 1)[-1]

            if item_name == item:
                checkbox = anchor.find_element(By.XPATH, ".//i[contains(@class, 'jstree-checkbox')]")
                driver.execute_script("arguments[0].click();", checkbox)
                break

        add_cart = driver.find_element("id", "add_cart")
        add_cart.click()
        sleep(4)
        Ok_button1 = driver.find_element(By.XPATH, "//button[@type='button' and @class='btn btn-primary bootbox-accept' and text()='OK']")
        Ok_button1.click()
        sleep(4)

    sleep(8)
    Next_button_1 = driver.find_element("id", "next_to_review_cart")
    Next_button_1.click()
    sleep(10)
    Next_button_2 = driver.find_element("id", "next_to_select_target")
    Next_button_2.click()
    sleep(6)
    to_env = driver.find_element(By.XPATH, f"//table[@id='target_env_table']//tr[td[2] = '{mapping_inputs[1].upper()}']//input[@type='checkbox']")
    to_env.click()
    sleep(4)

    Next_button_3 = driver.find_element("id", "next_to_submit_approval")
    Next_button_3.click()
    sleep(4)
    Next_button_4 = driver.find_element(By.XPATH, "//i[contains(@class, 'fa-hand-o-right') and @onclick='to_edit_mapping()']")
    Next_button_4.click()
    sleep(6)
    Ok_button_1 = driver.find_element(By.XPATH, "//button[text()='OK']")
    Ok_button_1.click()
    sleep(4)
    Host_group = driver.find_element(By.XPATH, f"//table[@id='host_apps_table']//tr[normalize-space(td[3]) = '{mapping_inputs[2].upper()}']//input[@type='checkbox']")
    Host_group.click()
    sleep(4)
    runtime_user = driver.find_element(By.XPATH, f"//table[@id='user_apps_table']//tr[td[3]='{mapping_inputs[3].lower()}']//input[@type='checkbox']")
    runtime_user.click()
    sleep(4)
    Variable_mapping = driver.find_element(By.XPATH, f"//table[@id='variable_apps_table']//tr[td[3]='{mapping_inputs[4].upper()}']//input[@type='checkbox']")
    Variable_mapping.click()
    sleep(4)
    Next_button_5 = driver.find_element(By.XPATH, f"//i[contains(@class, 'fa fa-hand-o-right') and @onclick='to_enable_disable()']")
    Next_button_5.click()
    sleep(4)
    Dropdown = driver.find_element(By.ID, "enable_or_disable")
    select = Select(Dropdown)
    select.select_by_visible_text(mapping_inputs[5].capitalize())
    sleep(4)
    Next_button_6 = driver.find_element(By.XPATH, "//i[contains(@class, 'fa fa-hand-o-right') and @onclick='to_submit_approval()']")
    Next_button_6.click()
    print("✅ All the jobs are added. Please review, add comments, and submit.")


def main():
    jobs = input("Enter the list of items separated by commas :").split(",")
    jobs = [item.strip() for item in jobs if item.strip()]
    mapping_inputs = input("Enter mapping details as comma separated values:").split(',')
    mapping_inputs = [value.strip() for value in mapping_inputs]
    max_items_per_driver = 30
    num_driver = math.ceil(len(jobs) / max_items_per_driver)
    print(f"Total items : {len(jobs)}. Launching {num_driver} Edge instances")

    chunks = [jobs[i:i + max_items_per_driver] for i in range(0, len(jobs), max_items_per_driver)]

    threads = []
    for i, chunk in enumerate(chunks):
        thread = Thread(target=add_jobs_to_cart, args=(chunk, mapping_inputs, i + 1))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
