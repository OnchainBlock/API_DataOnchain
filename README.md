# API_DataOnchain
Welcome to the public repository for OnchainBlock Backend.

## Description

This repository contains the source code and related documentation for OnchainBlock Backend, a backend project written in the Python programming language that provides APIs for OnchainBlock applications. This project is built using the FastAPI framework to handle HTTP requests and interact with the Postgres database management system. The project is organized with three main packages: data, api and libs.

## Access

Access to this repository is granted on a confidential basis and is intended solely for the use of authorized individuals or entities. If you have received access to this repository, you are responsible for ensuring the protection of its contents and adhering to any confidentiality or non-disclosure agreements in place.

  

## Proprietary Information

  

The source code and other materials within this repository are considered proprietary and confidential. You are strictly prohibited from sharing, reproducing, distributing, or disclosing the contents of this repository to any unauthorized individuals or entities. Any use of this source code or related materials beyond the scope of the authorized purpose is in violation of the terms of access.

  

## Licensing

  

This repository is not open-source and is subject to specific licensing terms as agreed upon between the owner and authorized users. Please refer to the LICENSE file within this repository for details on the terms of use.

  

## Features

- Allocation of Stablecoin Funds in Centralized Exchanges
- Allocation of Stablecoin in Decentralized Exchanges (DEXs)
- Stablecoin Flows across Bridges
- Ethereum (ETH) Statistics on Centralized Exchanges
- Total Value Locked in Ethereum on Layer 2s
- Real-time event processing using the data package

 
## Installation and Setup

  

To install and set up the project locally, follow these steps:

  

1. Ensure you have [python](https://www.python.org/) installed on your machine.

2. Clone the repository:

```python
git clone https://github.com/OnchainBlock/API_DataOnchain.git
```
3. Navigate to the project directory:

```python
cd API_DataOnchain
```
4. create virtual environment using python3
```python
windows instructions: 
python3 -m venv venv
venv\Scripts\activate 
```
```python
pip3 freeze > requirements.txt
```
 ```python
python3 -m pip install -r requirements.txt 
```
Linux instructions:
```python
python3 -m venv venv
source venv/bin/activate
```
```python
pip3 freeze > requirements.txt
```
 ```python
python3 -m pip install -r requirements.txt 
```




5. Configure the environment variables by creating a .env file in both the data and api directories within the apps directory. Use the provided .env.example file as a reference and input the necessary values.


6. Start the backend server quickly:
 ```python
cd API_DataOnchain
python3 main.py
```
   

The server should now run by default at http://localhost:8000/docs. 
  
## API Documentation

For detailed information about the available APIs and how to interact with them, you can read all API descriptions from yours Swagger UI Playground.


## Project Structure

The project is organized as follows:

- Backend: Automatic data model documentation with JSON Schema from FastAPI framework in python language

- worker: Listens for new blocks and handles events or automatically executes as a cron job after a certain period of time.

- libs: Contains common code such as models, data processors from datahouse


## Configuration

The project uses environment variables for configuration. Create an .env file in the application folder and fill in the required values, in this section we are only processing data from DataHouse, meaning transferred ELT (Extract, Load and Transform) from Datalake. The following variables are used, here the database is private if you want to see the data please [contact](contact@onchainblock.xyz)

- server_PATH: The URI for connecting to the Postgres database.
- table_PATH: The URI for connecting to table from database

## Licensing

This repository is not open-source and is subject to specific licensing terms as agreed upon between the owner and authorized users. Please refer to the LICENSE file within this repository for details on the terms of use.

  
## Contact

If you have any questions or need further clarification about the terms of access or licensing, please contact the repository owner at [contact](contact@onchainblock.xyz). By accessing this repository, you acknowledge and agree to abide by the terms and conditions outlined above. Thank you for your cooperation and understanding.
