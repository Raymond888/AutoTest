#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.common.dataset import DataSet


sql_create = '''CREATE TABLE {user}."{table}" (
  "id" NUMBER(38) primary key ,
  "name" CLOB ,
  "address" CLOB ,
  "phone_number" CLOB ,
  "email" CLOB ,
  "ip" CLOB 
)
TABLESPACE "EXPORT_DB"
LOGGING
NOCOMPRESS
PCTFREE 10
INITRANS 1
STORAGE (
  INITIAL 65536 
  NEXT 1048576 
  MINEXTENTS 1
  MAXEXTENTS 2147483645
  BUFFER_POOL DEFAULT
)
PARALLEL 1
NOCACHE
DISABLE ROW MOVEMENT
'''


sql_insert = '''INSERT INTO {user}."{table}" VALUES ('1', 'Rebecca', '366 Rivera Turnpike Apt', '13831001578', 'donald34@hotmail.com', '192.31.208.32')'''

sql_prepare = '''INSERT INTO {user}."{table}" ("id", "name", "address", "phone_number", "email", "ip") VALUES (:1, :2, :3, :4, :5, :6)'''


faker = DataSet().faker()

ids = range(1, 101)
names = [faker.name() for _ in range(100)]
addresses = [faker.address() for _ in range(100)]
phone_numbers = [faker.phone_number() for _ in range(100)]
emails = [faker.email() for _ in range(100)]
ips = [faker.ipv4_public() for _ in range(100)]

sql_data = [(id, name, address, phone_number, email, ip) \
            for (id, name, address, phone_number, email, ip) in zip(ids, names, addresses, phone_numbers, emails, ips)]


