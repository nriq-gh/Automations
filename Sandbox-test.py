#imports
import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.common.exceptions import ElementClickInterceptedException
import requests

#Slack webhook
slack_webhook_url = os.getenv('SLACK_WEBHOOK') #slack webhook

# Initialize the driver setup
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # For running without GUI
driver = webdriver.Chrome(service=Service(executable_path='/usr/bin/chromedriver'), options=options)

 # Define the values to match
requested_by_value = "test@test.com" #Username
customer_value = "SandboxTest" #NameoftheMachine

# Function to send the alert
def send_alert(status):
    print(f"Alert: The instance status is {status}.")
    message = {
        "text": f"Alert: The instance status is {status}."
    }
    try:
        response = requests.post(slack_webhook_url, json=message)
        response.raise_for_status()
        print(f"Slack message sent successfully. Status Code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to send Slack message: {e}")

# Login function
def login():
    password=os.getenv('PASSWORD_SANDBOX')
    driver.get("https://test.github.io/test-sandbox/") #link to the login page
    WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By.NAME, "email")))
    driver.find_element(By.NAME, "email").send_keys("sandbox-web@test.io") #username
    driver.find_element(By.NAME, "password").send_keys(password)
    driver.find_element(By.XPATH, "//button[contains(text(),'Login')]").click()

# Request an instance
def request_instance(driver):
    WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Request Instance')]")))
    driver.find_element(By.XPATH, "//button[contains(text(), 'Request Instance')]").click()

# Fill out the instance request form
def fill_request_form(driver, service, memory):
    WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By.NAME, "email")))
    driver.find_element(By.NAME, "email").send_keys("test@test.com")
    driver.find_element(By.NAME, "customer").send_keys("SandboxTest")
    select_region = Select(driver.find_element(By.ID, 'region'))
    select_region.select_by_visible_text('Shibuya, JP')
    select_services = Select(driver.find_element(By.ID, 'configTemplate'))
    select_services.select_by_visible_text(service)
    select_version = Select(driver.find_element(By.ID, 'version'))
    select_version.select_by_visible_text('sandbox-qa')
    select_memory = Select(driver.find_element(By.ID, 'machineSize'))
    select_memory.select_by_visible_text(memory)
    driver.find_element(By.XPATH, "//button[contains(text(),'Submit')]").click()

# Click Refresh button
def refresh():
    retries = 3
    for attempt in range(retries):
        try:
            refresh_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Refresh')]"))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", refresh_button)
            time.sleep(2)  # Slight delay to ensure the element is in view

            # Try to click the button with JavaScript
            driver.execute_script("arguments[0].click();", refresh_button)
            time.sleep(10)
            return  # Exit the function if the click was successful
        except ElementClickInterceptedException as e:
            if attempt < retries - 1:
                print(f"ElementClickInterceptedException encountered: {e}. Retrying... (Attempt {attempt + 1}/{retries})")
                time.sleep(2)  # Wait before retrying
                driver.execute_script("window.scrollBy(0, -100);")  # Scroll up by 100 pixels
            else:
                print(f"Failed after {retries} attempts due to ElementClickInterceptedException.")
                raise  # Rethrow the exception if the last attempt also fails
        except Exception as e:
            print(f"An error occurred: {e}")
            raise

# Find the status
def get_status(driver, email: str, customer: str) -> str:
    try:
        WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "table tbody")))
        instance_rows = driver.find_elements(By.CSS_SELECTOR, "tbody tr")
        
        if not instance_rows:
            print("Debug: No rows found in the table.")
            return None

        for row in instance_rows:
            driver.execute_script("arguments[0].scrollIntoView(true);", row)
            time.sleep(2)  # Slight delay for dynamic content

            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) < 6:  # Assuming there are at least 6 columns
                print("Debug: Insufficient columns in a row.")
                continue

            email_text = cells[1].text.strip().lower()  # Normalize to lowercase and strip spaces
            customer_text = cells[2].text.strip().lower()  # Normalize to lowercase and strip spaces
            #print(f"Debug: Comparison Email={email.lower().strip()} vs Row Email={email_text}, Comparison Customer={customer.lower().strip()} vs Row Customer={customer_text}")

            if email.lower().strip() == email_text and customer.lower().strip() == customer_text:
                return cells[5].text

        print("Debug: No matching email/customer found.")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None

#Delete the instance
def delete_entries(driver, requested_by_value: str, customer_value: str):
    try:
        # Define the XPath to find the delete button for matching entries
        xpath = f"//tr[td[@data-cell-id='header_requestedBy']/p[text()='{requested_by_value}'] and td[@data-cell-id='header_customer']/p[text()='{customer_value}']]//button[@aria-label='Delete Instance']"
        delete_buttons = driver.find_elements(By.XPATH, xpath)
        if not delete_buttons:
            print("No delete buttons found")
            return

        for button in delete_buttons:
            # Scroll the button into view
            driver.execute_script("arguments[0].scrollIntoView(true);", button)
            time.sleep(10)  # Allow some time for the page to adjust and the element to become clickable
            button.click()
            time.sleep(10)  # Wait for modal or confirmation dialog to appear

            # Click the confirmation button to finalize deletion
            confirm_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Delete')]"))
            )
            confirm_button.click()
            time.sleep(5)  # Wait for the action to complete
    except TimeoutException:
        print("Failed to find or click on the delete or confirm button.")
    except NoSuchElementException:
        print("Failed to locate an element in the deletion process.")

# Main execution function
def main():
    try:
        login()
        request_instance(driver)
        fill_request_form(driver, "All Services", "16GB")
        time.sleep(1500)  # Wait for 25 minutes
        refresh()
        status = get_status(driver, requested_by_value, customer_value)
        if status is None:
            print("The instance was not created.")
            send_alert(status)
            return  # Exit or handle the absence of status as needed
        if "Initializing" in status:
            time.sleep(600)  # Wait additional 10 minutes if initializing
            refresh()
        elif "Ready" in status:
            print("Instance is.", status)
            delete_entries(driver, requested_by_value, customer_value)
            send_alert(status) #just for test
        else:
            print("Instance is.", status)
            #delete_entries(driver, requested_by_value, customer_value)
            send_alert(status)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
