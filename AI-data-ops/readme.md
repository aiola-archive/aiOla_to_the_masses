# aiOla AI-data-ops

## Overview
Welcome to aiOla's AI-data-ops solution, an integral part of our suite of AI solutions. aiOla is a groundbreaking AI company that democratizes access to cutting-edge speech-to-text and natural language processing technologies. Our solutions serve a broad range of industries including food, beverages, commerce, oil and gas, enabling them to enhance productivity, drive analytics, and grow sustainably.

This repository contains all the necessary code and documentation to set up and run our model monitoring and human review systems, designed to streamline the deployment and maintenance of AI models.

## Sub-Projects

This main project is divided into two key sub-projects, each with its own README for specific instructions:

1. **[Daily Tasks Orchestrator](./Daily-Tasks-Orchestrator/README.md)**: A script designed to be a daily monitoring tool for model events. It scans new events, evaluates them against specified conditions, and triggers human review tasks using Amazon SageMaker's Augmented AI (A2I) service if the conditions are met.

2. **[Deployment Scripts](./Deployment-Scripts/README.md)**: A collection of Python scripts to be executed in a particular order for the initial setup of tables, workflows, and user pools for Amazon SageMaker A2I and AWS Cognito.

## Getting Started

### Pre-requisites

- AWS Account
- Python 3.x
- Required Python Packages (Install via `pip install -r requirements.txt`)

### Installation

Please refer to the README files of the individual sub-projects for detailed installation and configuration instructions:

- [Daily Tasks Orchestrator README](./Daily-Tasks-Orchestrator/README.md)
- [Deployment Scripts README](./Deployment-Scripts/README.md)

## About aiOla

At aiOla, we are committed to bringing the power of AI to the masses. Our products empower companies across various industries to be more analytics-driven, increase productivity, and leverage AI to solve complex problems.

For more information about aiOla and our offerings, visit [our website](https://aiola.com).

