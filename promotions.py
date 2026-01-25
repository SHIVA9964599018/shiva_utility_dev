from selenium import webdriver
from selenium.webdriver.common.by import By
from time import sleep
from threading import Thread
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import math


def add_jobs_to_cart(jobs, mapping_inputs, driver_index):
    options = webdriver.ChromeOptions()
    options.add_experimental_option("detach", True)
    driver = webdriver.Chrome(options=options)
    driver.get("https://ctmweb-prd1-01.cisco.com:8443/mapping_rules/folder_promotion")

    input(
        f"Driver {driver_index} is ready. Authenticate the webpage and then press enter to start adding jobs to the cart..")

    Advanced_promotion = driver.find_element(By.LINK_TEXT, "Advanced Promotion")
    Advanced_promotion.click()
    sleep(1)
    Folder_promotion_request = driver.find_element(By.LINK_TEXT, "Folder Promotion Request")
    Folder_promotion_request.click()
    sleep(1)
    New_button = driver.find_element(By.XPATH, "//span[@class='icon-text' and text()='New']")
    New_button.click()
    sleep(1)
    Ok_button = driver.find_element(By.XPATH,
                                    "//button[@type='button' and @class='btn btn-primary bootbox-accept' and text()='OK']")
    Ok_button.click()
    sleep(1)
    # Locate the radio button for "DEV_BASE" and click it

    from_env = driver.find_element(By.XPATH,
                                   f"//table[@id='source_env_table']//tr[td[text()='{mapping_inputs[0]}']]//input[@type='radio']")
    from_env.click()
    sleep(1)

    Next_button = driver.find_element(By.XPATH, "//span[@class='icon-text' and text()='Next']")
    Next_button.click()
    sleep(4)

    for item in jobs:
        search_bar = driver.find_element("id", "folder")
        search_bar.clear()
        search_bar.send_keys(item)
        search_button = driver.find_element(By.XPATH, "//i[@class='fa fa-search']")
        search_button.click()
        sleep(5)

        # Locate all search results
        result_items = driver.find_elements(By.XPATH, "//div[@id='data']//li[@role='treeitem']")

        # Scenario: No search results
        if not result_items:
            continue

        for result in result_items:
            anchor = result.find_element(By.XPATH, ".//a[@class='jstree-anchor']")
            item_name = anchor.text.strip()
            item_name = item_name.split(":", 1)[-1]

            if item_name == item:
                checkbox = anchor.find_element(By.XPATH, ".//i[contains(@class, 'jstree-checkbox')]")
                driver.execute_script("arguments[0].click();", checkbox)
                break
        # sleep(3)
        add_cart = driver.find_element("id", "add_cart")
        add_cart.click()
        sleep(1)
        Ok_button1 = driver.find_element(By.XPATH,
                                         "//button[@type='button' and @class='btn btn-primary bootbox-accept' and text()='OK']")
        Ok_button1.click()
        sleep(1)

    sleep(2)
    Next_button_1 = driver.find_element("id", "next_to_review_cart")
    Next_button_1.click()
    sleep(3)
    Next_button_2 = driver.find_element("id", "next_to_select_target")
    Next_button_2.click()
    sleep(2)
    to_env = driver.find_element(By.XPATH,
                                 f"//table[@id='target_env_table']//tr[td[2] = '{mapping_inputs[1]}']//input[@type='checkbox']")
    to_env.click()
    sleep(1)
    By.XPATH, "//table[@id='target_env_table']//tr[td[2] = 'STG_BASE']//input[@type='checkbox']"
    Next_button_3 = driver.find_element("id", "next_to_submit_approval")
    Next_button_3.click()
    sleep(1)
    Next_button_4 = driver.find_element(By.XPATH,
                                        "//i[contains(@class, 'fa-hand-o-right') and @onclick='to_edit_mapping()']")
    Next_button_4.click()
    sleep(2)
    Ok_button_1 = driver.find_element(By.XPATH, "//button[text()='OK']")
    Ok_button_1.click()
    sleep(1)
    Host_group = driver.find_element(By.XPATH,
                                     f"//table[@id='host_apps_table']//tr[normalize-space(td[3]) = '{mapping_inputs[2]}']//input[@type='checkbox']")
    Host_group.click()
    sleep(1)
    runtime_user = driver.find_element(By.XPATH,
                                       f"//table[@id='user_apps_table']//tr[td[3]='{mapping_inputs[3]}']//input[@type='checkbox']")
    runtime_user.click()
    sleep(1)
    Variable_mapping = driver.find_element(By.XPATH,
                                           f"//table[@id='variable_apps_table']//tr[td[3]='{mapping_inputs[4]}']//input[@type='checkbox']")
    Variable_mapping.click()
    sleep(1)
    Next_button_5 = driver.find_element(By.XPATH,
                                        f"//i[contains(@class, 'fa fa-hand-o-right') and @onclick='to_enable_disable()']")
    Next_button_5.click()
    sleep(1)
    Dropdown = driver.find_element(By.ID, "enable_or_disable")
    select = Select(Dropdown)
    select.select_by_visible_text(mapping_inputs[5])
    sleep(1)
    Next_button_6 = driver.find_element(By.XPATH,
                                        "//i[contains(@class, 'fa fa-hand-o-right') and @onclick='to_submit_approval()']")
    Next_button_6.click()
    print("All the jobs are added please review, add comments and submit")


def main():
    jobs = input("Enter the list of items seperated by commas :").split(",")
    jobs = [item.strip() for item in jobs if item.strip()]
    mapping_inputs = input("enter mapping details as comma seperated values:")
    mapping_inputs = [value.strip() for value in mapping_inputs.split(',')]
    max_items_per_driver = 30
    num_driver = math.ceil(len(jobs) / max_items_per_driver)
    print(f"Total items : {len(jobs)}. Launching {num_driver} chrome instances")

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





