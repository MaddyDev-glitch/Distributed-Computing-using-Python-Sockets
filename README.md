# Distributed Computing with Python Sockets

This project demonstrates a basic implementation of distributed computing using Python's socket programming. It showcases how a master node can distribute tasks to multiple slave nodes and aggregate the computed results. This example is intended for educational purposes to understand the fundamentals of network programming and distributed computing architectures.

## Overview

The project consists of two main components: the master node and the slave nodes. The master node sends out tasks to the slave nodes through a network using socket programming. The slave nodes receive the tasks, compute the results, and send the results back to the master node. This process highlights a simple yet effective way of distributing computational load across multiple machines.

## Features

- Basic implementation of client-server architecture using Python's `socket` module.
- Example task distribution and result aggregation logic.
- Simple and understandable codebase for beginners in network programming and distributed computing.

## Installation

To run this project, you need Python 3.6 or later. Clone the repository to your local machine using:

```bash
git clone https://github.com/yourusername/distributed-computing-python-sockets.git
cd distributed-computing-python-sockets
```