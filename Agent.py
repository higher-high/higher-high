import requests
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from langchain import AgentExecutor, Tool
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain.memory import ConversationBufferMemory
import subprocess
import gradio as gr

# Base URL for YARN ResourceManager
YARN_BASE_URL = "http://<your-yarn-host>:8088/ws/v1/cluster"  # Replace <your-yarn-host> with your actual YARN host

# Kerberos Authentication Setup (using principal and keytab)
KERBEROS_PRINCIPAL = "your_principal@YOUR.REALM"
KEYTAB_PATH = "/path/to/your.keytab"
subprocess.run(["kinit", "-kt", KEYTAB_PATH, KERBEROS_PRINCIPAL])

# Function to submit a Spark Job
def submit_spark_job(app_name, app_args):
    url = f"{YARN_BASE_URL}/apps/new-application"
    response = requests.post(url, auth=HTTPKerberosAuth())
    if response.status_code == 200:
        app_id = response.json()["application-id"]
        data = {
            "application-id": app_id,
            "application-name": app_name,
            "am-container-spec": {
                "commands": app_args
            },
            "application-type": "SPARK",
        }
        submit_url = f"{YARN_BASE_URL}/apps"
        headers = {'Content-Type': 'application/json'}
        submit_response = requests.post(submit_url, data=json.dumps(data), headers=headers, auth=HTTPKerberosAuth())
        return submit_response.json()
    else:
        return response.text

# Function to kill a Spark Job
def kill_spark_job(app_name):
    app_id = get_app_id_by_name(app_name)
    if not app_id:
        return f"Application with name {app_name} not found."
    url = f"{YARN_BASE_URL}/apps/{app_id}/state"
    data = {"state": "KILLED"}
    headers = {'Content-Type': 'application/json'}
    response = requests.put(url, data=json.dumps(data), headers=headers, auth=HTTPKerberosAuth())
    return response.json()

# Function to get Spark Job status
def get_job_status(app_name):
    app_id = get_app_id_by_name(app_name)
    if not app_id:
        return f"Application with name {app_name} not found."
    url = f"{YARN_BASE_URL}/apps/{app_id}"
    response = requests.get(url, auth=HTTPKerberosAuth())
    if response.status_code == 200:
        return response.json()
    else:
        return response.text

# Function to get the error message for a failed job
def get_job_error(app_name):
    job_details = get_job_status(app_name)
    if isinstance(job_details, str):
        return job_details
    if job_details.get("finalStatus", "") == "FAILED":
        return job_details.get("diagnostics", "No diagnostics available.")
    else:
        return "Job is not in FAILED state."

# Function to get the error log for a job
def get_job_error_log(app_name):
    app_id = get_app_id_by_name(app_name)
    if not app_id:
        return f"Application with name {app_name} not found."
    url = f"{YARN_BASE_URL}/apps/{app_id}/logs"
    response = requests.get(url, auth=HTTPKerberosAuth())
    if response.status_code == 200:
        return response.text
    else:
        return "Failed to retrieve error log."

# Function to get the application ID by application name
def get_app_id_by_name(app_name):
    url = f"{YARN_BASE_URL}/apps"
    response = requests.get(url, auth=HTTPKerberosAuth())
    if response.status_code == 200:
        apps = response.json().get("apps", {}).get("app", [])
        for app in apps:
            if app.get("name") == app_name:
                return app.get("id")
    return None

# Function to get a list of running job names for a specific user
def get_running_jobs_by_user(user):
    url = f"{YARN_BASE_URL}/apps?state=RUNNING"
    response = requests.get(url, auth=HTTPKerberosAuth())
    running_jobs = []
    if response.status_code == 200:
        apps = response.json().get("apps", {}).get("app", [])
        for app in apps:
            if app.get("user") == user:
                running_jobs.append(app.get("name"))
    return running_jobs if running_jobs else f"No running jobs found for user {user}."

# Function to send an email to the support team
def send_error_email(app_name, error_message):
    sender_email = "your_email@example.com"
    recipient_email = "support_team@example.com"
    smtp_server = "smtp.example.com"
    smtp_port = 587
    smtp_user = "your_email@example.com"
    smtp_password = "your_password"

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = f"Spark Job {app_name} Failed"

    body = f"The Spark job with application name {app_name} has failed. Error details: {error_message}"
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(sender_email, recipient_email, msg.as_string())
        server.quit()
        print(f"Email sent successfully to {recipient_email}")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")

# LangChain Setup for Llama model via Ollama
class OllamaLLM:
    def __init__(self, api_url):
        self.api_url = api_url

    def call(self, prompt):
        response = requests.post(
            self.api_url,
            json={"prompt": prompt}
        )
        if response.status_code == 200:
            return response.json().get("completion", "")
        else:
            return f"Error: {response.status_code}"

ollama_api_url = "http://<your-ollama-api-url>/v1/chat"  # Replace with your actual Ollama API URL
llm = OllamaLLM(api_url=ollama_api_url)
memory = ConversationBufferMemory()

# Define tools for LangChain
def submit_job_tool(app_name, app_args):
    result = submit_spark_job(app_name, app_args)
    return f"Submission result: {result}"

def kill_job_tool(app_name):
    result = kill_spark_job(app_name)
    return f"Kill result: {result}"

def job_status_tool(app_name):
    result = get_job_status(app_name)
    return f"Job status: {result}"

def job_error_tool(app_name):
    result = get_job_error(app_name)
    return f"Job error: {result}"

def job_error_log_tool(app_name):
    result = get_job_error_log(app_name)
    return f"Job error log: {result}"

def running_jobs_tool(user):
    result = get_running_jobs_by_user(user)
    return f"Running jobs for user {user}: {result}"

def send_email_tool(app_name, error_message):
    send_error_email(app_name, error_message)
    return f"Email sent for application name {app_name}"

# Create LangChain Tools
tools = [
    Tool(
        name="Submit Spark Job",
        func=submit_job_tool,
        description="Use this to submit a new Spark job with application name and arguments."
    ),
    Tool(
        name="Kill Spark Job",
        func=kill_job_tool,
        description="Use this to kill a Spark job given the application name."
    ),
    Tool(
        name="Get Spark Job Status",
        func=job_status_tool,
        description="Use this to get the status of a Spark job given the application name."
    ),
    Tool(
        name="Get Spark Job Error",
        func=job_error_tool,
        description="Use this to get the error message of a failed Spark job given the application name."
    ),
    Tool(
        name="Get Spark Job Error Log",
        func=job_error_log_tool,
        description="Use this to get the error log of a Spark job given the application name."
    ),
    Tool(
        name="Get Running Jobs by User",
        func=running_jobs_tool,
        description="Use this to get a list of running Spark jobs for a specific user."
    ),
    Tool(
        name="Send Error Email",
        func=send_email_tool,
        description="Use this to send an error email for a Spark job given the application name and error message."
    )
]

# Define the prompt template
prompt_template = PromptTemplate(
    input_variables=["input", "tool_output"],
    template="""
    You are an assistant that helps with managing Spark jobs. The user will provide natural language requests, and you will interpret the commands and use the appropriate tool to execute them.
    Request: {input}
    Tool Output: {tool_output}
    Based on the tool output, provide a helpful response to the user.
    """
)

llm_chain = LLMChain(
    llm=llm,
    prompt=prompt_template,
    memory=memory
)

# Create the agent executor
class CustomAgentExecutor(AgentExecutor):
    def run(self, user_input):
        tool_output = super().run(user_input)
        response = llm_chain.run({"input": user_input, "tool_output": tool_output})
        return response

agent = CustomAgentExecutor.from_llm_and_tools(
    llm_chain=llm_chain,
    tools=tools,
    verbose=True
)

# Gradio Web UI
def interact_with_agent(user_input):
    return agent.run(user_input)

gui = gr.Interface(fn=interact_with_agent, inputs="text", outputs="text", title="Spark Job Management Assistant")

gui.launch()

# Example Usage
if __name__ == "__main__":
    # Launch the Gradio Web UI
    gui.launch()
