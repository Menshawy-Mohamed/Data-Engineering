#!/usr/bin/env python
# coding: utf-8

# In[1]:


import time
import random
import socket
from datetime import datetime


# In[2]:


# Sample data
ips = ["127.0.0.1", "192.168.8.56", "10.0.1.8", "192.168.1.10", "10.0.0.5"]
urls = ["/", "/index.html", "/about", "/contact", "/login"]
methods = ["GET", "POST", "PULL"]
statuses = [200, 404, 500, 302]
agents = ["curl/7.68.0", "Mozilla/5.0", "PostmanRuntime/7.28.0"]

# Flume agent address
FLUME_HOST = "localhost"
FLUME_PORT = 44444  # Same port used in Flume config


# In[3]:


def generate_log():
    ip = random.choice(ips)
    time_str = datetime.now().strftime("%d/%b/%Y:%H:%M:%S %z")
    method = random.choice(methods)
    url = random.choice(urls)
    status = random.choice(statuses)
    size = random.randint(100, 5000)
    agent = random.choice(agents)
    return f'{ip} - - [{time_str}] "{method} {url} HTTP/1.1" {status} {size} "-" "{agent}"\n'


# In[ ]:


# Send logs to Flume
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((FLUME_HOST, FLUME_PORT))
    while True:
        log_line = generate_log()
        s.sendall(log_line.encode())
        print(log_line.strip())
        time.sleep(1)


# In[ ]:




