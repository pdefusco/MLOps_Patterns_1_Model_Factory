# MLOps Patterns: Model Factory

# This project is WIP

## Objective

This demonstration provides a reference for implementing a Model Factory in Cloudera Machine Learning. The github repository provides all the artifacts required for reproducing the example in a CML Project.  

A Model Factory is a common Machine Learning Engineering pattern to enable the continuous creation and maintenance of production models. It consists of a sequential workflow to incrementally refine models as new datasets and features are created.

Key benefits include the ability to deploy and scale machine learning use cases faster and at lower cost; the ability to turn a model concept into a long term value generating asset in a large organization; and the ability to free up Data Scientists and Engineers hours so they can focus on new use cases rather than maintaining existing ones.  

CML has extensive MLOps features and a set of model and lifecycle management capabilities to enable the repeatable, transparent, and governed approaches necessary for scaling model deployments and ML use cases.

## Requirements

This project requires a CML Workspace (Private and Public Cloud ok) or a CDSW Cluster.
Step by step instructions to reproduce in your own environment are included below.

# Defining the Model Factory

The Model Factory pattern relies on the below foundations:

* Heavy reliance on standardization and "dev ops" style continuous operations in the context of a governed, reproducible, and highly observable ML Ops environment.
* An incremental workflow starting with a clear baseline and a sequential workflow of model versions improving upon the baseline.
* A continuous monitoring system that can flag outdated model versions with automated tests (e.g. an A/B test) based on logged model performance metrics.
* Optionally, using pre-trained machine learning models, especially in the initial phases such as when deploying a model baseline.

Some of the benefits of implementing this pattern in CML include:

* Model cataloging and lineage capabilities to allow visibility into the entire ML lifecycle, which eliminates silos and blind spots for full lifecycle transparency, explainability, and accountability.
* Full end-to-end machine learning lifecycle management that includes everything required to securely deploy machine learning models to production, ensure accuracy, and scale use cases.
* An extensive model monitoring service designed to track and monitor both technical aspects and accuracy of predictions in a repeatable, secure, and scalable way.
* New MLOps features for monitoring the functional and business performance of machine learning models such as detecting model performance and drift over time with native storage and access to custom and arbitrary model metrics; measuring and tracking individual prediction accuracy, ensuring models are compliant and performing optimally.
* Increased model security for Model REST endpoints, which allows models to be served in a CML production environment without compromising security.
* Full integration with the Cloudera Data Platform. CDP is an enterprise data cloud with support for an environment running both on on-premises and in a public cloud setup (Hybrid Cloud). CDP also has multi-cloud and multifunction capabilities.
