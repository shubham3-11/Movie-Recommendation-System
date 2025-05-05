# Movie-Recommendation-System

This repository documents the design, development, deployment, and evaluation of a production-grade movie recommendation system built for the CMU Software Engineering for AI (SEAI) course. Our system uses collaborative filtering techniques (SVD and KNN) along with robust engineering practices to support deployment, monitoring, and fairness evaluation.
🔧 **Project Overview**
The goal was to design a scalable, reliable, and fair movie recommendation engine that can be served via a backend API. We progressively enhanced the system across four milestones, covering modeling, deployment, evaluation, and ethical considerations.

📍**Milestone 1: Modeling and First Deployment**
Objective: Develop initial recommendation models and set up a basic backend service.
Data Sources
User Watch Data – implicit feedback (watch time)
User Rating Data – explicit feedback (1–5 ratings)
Movie Metadata – titles, genres, release dates
User Metadata – age, gender, occupation
Models Implemented
SVD (Singular Value Decomposition): Matrix factorization to learn latent user-movie features.
KNN with Cosine Similarity: Neighborhood-based collaborative filtering.
GCN (Graph Convolutional Network): Bipartite graph modeling to capture higher-order interactions.
Deployment
Flask-based API deployed to serve recommendations.
Predictions generated and returned based on trained model outputs.

📍 **Milestone 2: Evaluation and Pipeline Implementation**
Objective: Evaluate the models both offline and online, and implement a data ingestion and training pipeline.
Offline Evaluation
Metrics: RMSE and MAE
Technique: 5-fold cross-validation using Surprise
Best Performance: SVD RMSE ≈ 0.3786, MAE ≈ 0.2741
Online Evaluation
Metrics:
Average Watch Time After Recommendation
Conversion Rate (users who watched a recommended movie)
Logs: Stored in MongoDB collections for telemetry.
Data Pipeline
Ingest data from Kafka and APIs → preprocess → store in MongoDB.
Training modules load data, train models, and expose prediction interfaces.
Evaluation scripts run regularly to compute online metrics.

📍 **Milestone 3: Automation, Monitoring, and A/B Testing**
Objective: Automate model updates, set up monitoring infrastructure, and perform experimental comparisons.
Automated Retraining
Models retrained every 3 days using fresh data from the past week.
Thread-safe in-memory model swapping without restarting the server.
Unique versioning for both model and data snapshots.
Monitoring Infrastructure
Prometheus – Collects metrics like latency and request success rate.
Grafana – Visualizes trends, sets up alerts for availability and performance.
Kafka Logs – Track recommendation requests and latency.
A/B Testing Framework
Load-balanced round-robin routing to SVD and KNN services.
Results logged in MongoDB with pipeline type and outcomes.
Statistical tests:
T-test for average watch time
Chi-squared test for conversion rate

📍 **Milestone 4: Fairness, Feedback Loops, and Security**
Objective: Identify and mitigate risks related to bias, feedback loops, and security vulnerabilities.
Fairness Analysis
Found genre and gender imbalances in training data.
RMSE for male users: 0.3221; for female users: 0.5102 → indicates fairness issue.
Suggested fairness-aware re-ranking and data rebalancing.
Feedback Loops
Detected popularity bias and preference drift through genre entropy analysis.
Proposed time-decayed interaction weights and diversity-enhancing MMR re-ranking.
Security Measures
Used OWASP Threat Dragon for STRIDE-based modeling.
Detected risks: DoS attacks, data poisoning.
Mitigation strategies: API authentication, request throttling, RBAC for MongoDB, encryption-at-rest.

🧠 **Key Technologies & Skills**
Python, Flask, MongoDB, Surprise, PyTorch, Docker, Prometheus, Grafana, Kafka, GitHub Actions, A/B Testing, CI/CD, STRIDE, OWASP Threat Dragon

📊 **Outcomes**
Modular, scalable microservices architecture
Zero-downtime model updates with version tracking
Online evaluation metrics show improvement in engagement
Statistical testing of experimental pipelines
Fairness violations identified and tracked
End-to-end monitoring and security enforcement

📁 **Repository Structure (Simplified)**
![image](https://github.com/user-attachments/assets/f65c730b-20eb-4028-907c-45ab1a6237b1)



