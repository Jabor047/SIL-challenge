# SIL Challenge
> Create ETL pipelines to load data from the different sources to BigQuery where data enrichment will be done. This ETL process should be automated and repeatable. All ETL scripts jobs should run on Google Cloud - and you are free to pick any technique. Load data from the different sources into one dataset (use your name for the dataset in Bigquery ) and save the enriched data into a BigQuery table ( in your dataset ). 

> You’ll then be expected to create a simple dashboard that visualizes the data. Explore the data and figure out what questions you can answer with the data. For this, you can connect any visualization tool of your choice e.g Data Studio, PowerBI, Tableau, Qlik Sense etc.

> As a final step, we’ll expect you to create a Jupyter notebook ( https://cloud.google.com/ai-platform-notebooks is an option) and attempt to train a simple ML model. One suggestion is to train a simple model that predicts the likelihood of an invoice being paid, on the basis of the historical data that you’ll have. This is not cast in stone - you can explore the data and train a different model. We’ll not be looking for a sophisticated production-grade model: have fun with it, we are just checking that you have some exposure to AI/ML.

- cloudfunction.py contains code that runs when the cloudfunction is called
- model_code.py contains the model notebook exported as a python