# Measurement-of-Various-Message-Transfer-Protocols
Performance Test of Message Transfer Protocols
<br>
Check this thesis (Korean) > [Abstract](https://github.com/passionleader/Message-Queue-Performance-test/files/8792619/P-39.pdf)  [Poster Thesis][P-39.pdf](https://github.com/passionleader/Message-Queue-Performance-test/files/8792620/P-39.pdf)


<br>

## Overall Architecture
![image](https://user-images.githubusercontent.com/55945939/144700399-0eb81e4c-2953-4f81-806b-45b8be726f11.png)

---

### Message Type
* If possible, transfer messages as Pandas DataFrame
* unless, convert DF to JSON
---
### Used Message Transfer Protocols
1. Apache Kafka
4. RabbitMQ
6. ActiveMQ
7. NATS
8. Redis(for transfer)
9. MongoDB Cient(for transfer)
---
### Used Databases
* MongoDB

---
### Process of Message Transfer 
![image](https://user-images.githubusercontent.com/55945939/144702140-192338a0-24ad-49ad-818e-9dc984491651.png)

### Measuring Time
![image](https://user-images.githubusercontent.com/55945939/144702156-e52d2466-3435-47c3-ad80-33fa9c1c30d4.png)

---

## acknowledgment
#### This study has been conducted with the support of the Korea Institute of Industrial Technology, KITECH

