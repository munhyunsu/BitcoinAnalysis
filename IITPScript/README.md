# IITP 과제 성과용 스크립트

## 공인인증시험

### 0. 공통

- 데이터베이스 구축

```
dbv3-index.db
dbv3-core.db
dbv3-util.db
dbv3-service.db
```

### 1. 가상자산 취급 업소 식별

1. 전처리

```bash
python3 f1-s1-preprocess.py --index dbv3-index.db --core dbv3-core.db --util dbv3-util.db --service dbv3-service.db --input f1-train.csv
```

2. 학습

```bash
python3 f1-s2-train.py --input f2-features.pkl
```

3. 예측 테스트

```bash
python3 f1-s3-test.py --index dbv3-index.db --core dbv3-core.db --util dbv3-util.db --service dbv3-service.db --model f1-model.pkl --input f1-test.csv
```

### 2. 부정거래 식별

1. 전처리

```bash
python3 f2-s1-preprocess.py --index dbv3-index.db --core dbv3-core.db --util dbv3-util.db --service dbv3-service.db --input f2-train.csv
```

2. 학습

```bash
python3 f2-s2-train.py --input f2-features.pkl
```

3. 예측 테스트

```bash
python3 f2-s3-test.py --index dbv3-index.db --core dbv3-core.db --util dbv3-util.db --service dbv3-service.db --model f2-model.pkl --input f2-test.csv
```


### 1. 가상자산 취급 업소 식별 without Database

1. 학습

```
```

2. 예측 테스트

```bash
```
