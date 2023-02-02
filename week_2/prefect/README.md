## Week 2 Homework

The goal of this homework is to familiarise users with workflow orchestration and observation. 


## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

* 447,770
* 766,792
* 299,234
* 822,132

``` bash
conda activate zoom
prefect orion start
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
python etl_web_to_gcs_hw.py
```
`clean` function prints the number of rows. 

Answer: 447770

## Question 2. Scheduling with Cron

Cron is a common scheduling specification for workflows. 

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

- `0 5 1 * *`
- `0 0 5 1 *`
- `5 * 1 0 *`
- `* * 5 1 0`

``` bash
prefect deployment build ./etl_web_to_gcs_hw.py:etl_web_to_gcs_hw -n "Web to GCS Flow (HW)"
prefect deployment apply etl_web_to_gcs_hw-deployment.yaml
prefect agent start --work-queue "default" 
```
Click Deployments> Your Deployment > Run > Quick Run
How to schedule:
Click on your deployment > Edit > Add schedule OR
`prefect deployment build ./etl_web_to_gcs_hw.py:etl_web_to_gcs_hw -n "Web to GCS Flow (HW) --cron "0 5 1 * *" -a`

Answer: `0 5 1 * *`


## Question 3. Loading data to BigQuery 

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color. 

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

- 14,851,920
- 12,282,990
- 27,235,753
- 11,338,483

``` bash
prefect deployment build ./etl_gcs_to_bq_hw.py:etl_gcs_to_bq_parent_flow -n "GCS to BQ Flow (HW)" --params '{"months": [2,3], "year": 2019, "color": "yellow"}'
prefect deployment apply etl_gcs_to_bq_parent_flow-deployment.yaml
prefect agent start --work-queue "default" 
```
Click Deployments> Your Deployment > Run > Quick Run

2019-02 (yellow)
Total number of processed rows: 7019375
2019-03 (yellow)
Total number of processed rows: 7832545

Answer: Total number of processed rows for 2019-02 and 2019-03: 14,851,920

## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

- 88,019
- 192,297
- 88,605
- 190,225

### Set up
```bash
pip install prefect-github
prefect block register -m prefect_github
```

### Create a GitHub block on Prefect Orion
Paste repo url in github block

### Build, apply and run deployment
```bash
prefect agent start --work-queue "default" 
prefect deployment build ./week_2/prefect/flows/04_homework/etl_web_to_gcs_hw.py:etl_web_to_gcs_hw \
-n "GitHub Storage Flow" \
-sb github/github-block \
-o ./week_2/prefect/flows/04_homework/etl_web_to_gcs_hw_github-deployment.yaml \
--apply
```

Click Deployments > Your Deployment > Run > Quick Run

*Note: prefect deployment build gave an error when there was airflow/logs/ in the github repo for unknown reason, delete before build to fix*

Answer: 88605

### When the file is in the repo

## Question 5. Email or Slack notifications

Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur. 

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up. 

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook. 

Join my temporary Slack workspace with [this link](https://join.slack.com/t/temp-notify/shared_invite/zt-1odklt4wh-hH~b89HN8MjMrPGEaOlxIw). 400 people can use this link and it expires in 90 days. 

In the Prefect Cloud UI create an [Automation](https://docs.prefect.io/ui/automations) or in the Prefect Orion UI create a [Notification](https://docs.prefect.io/ui/notifications/) to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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

Test the functionality.

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create. 

How many rows were processed by the script?

- `125,268`
- `377,922`
- `728,390`
- `514,392`

### Set up Slack notification on Prefect Orion UI.
Create Slack App (via browser) and connect it to workspace and channel and output will be an incoming webhook
Paste this incoming webhook to the Slack Notification on Prefect Orion. 
https://hooks.slack.com/services/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
Run the targeted flow.

This notification was triggered on slack channel
test-notify-app `APP`
```
Prefect flow run notification
Flow run etl-web-to-gcs-hw/tourmaline-boa entered state Completed at 2023-02-02T06:30:25.455427+00:00.
Flow ID: a7de22d4-1828-41d8-a4ec-d68a9b8e187f
Flow run ID: 75b4eccb-aa0a-407d-a093-4becfe69c731
Flow run URL: http://127.0.0.1:4200/flow-runs/flow-run/75b4eccb-aa0a-407d-a093-4becfe69c731
State message: All states completed.
```

### Set up email notification on Prefect Cloud UI.
```bash
prefect cloud login
prefect block register -m prefect_gcp
```
Create GCP Credentials Block on Prefect Cloud but GCP Bucket Block is built in `etl_web_to_gcs_hw_q5.py`
`prefect deployment build ./etl_web_to_gcs_hw_q5.py:etl_web_to_gcs_hw -n "web_to_gcs_hw_q5_flow" --apply -o etl_web_to_gcs_hw_q5-deployment.yaml`
`prefect agent start -q 'default'`
Prefect Cloud: Click Deployments > Your Deployment > Run > Quick Run

Email notification from: no-reply@prefect.io
```
Flow run etl-web-to-gcs-hw/pygmy-caracal entered state `Completed` at 2023-02-02T13:11:14.248460+00:00.
Flow ID: 2b4efee0-f4ab-44c8-8e0d-c3afe677b47d
Flow run ID: b0a0b5d9-9772-41a4-9cd2-683061084e99
Flow run URL: https://app.prefect.cloud/account/0f414be3-3ec3-4ab0-9e6e-2c81cbee86c4/workspace/7c19bdd8-4a12-4e42-b1d3-e7ee66029510/flow-runs/flow-run/b0a0b5d9-9772-41a4-9cd2-683061084e99
State message: All states completed.
```

*Once you're connected to Prefect Cloud, it seems that the deployments will only show on Prefect Cloud UI and not on Prefect Orion UI.*

Answer: 514392

## Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

- 5
- 6
- 8
- 10

`********`
Answer: 8
## Submitting the solutions

* Form for submitting: https://docs.google.com/forms/d/e/1FAIpQLSf8g4qz6JnHmCPslWK5ZPyjmoK6DtRK_5vLCO6pqXPscS4b7Q/viewform
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 6 February (Monday), 22:00 CET
