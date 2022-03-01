# BitSQL 데이터베이스 구축 방법 (2차년도 기준) Updated: 2022-02-27

## 필수 소프트웨어 다운로드 및 설치

1. [Bitcoin Core](https://bitcoin.org/en/download) 다운로드

2. [Python 3](https://www.python.org/downloads/) 다운로드

## Bitcoin Core 세팅

1. Bitcoin Core 첫 실행 후 종료 (설정 디렉터리 및 파일 생성)

2. Bitcoin Core 설정파일 [다운로드](https://github.com/bitcoin/bitcoin/blob/master/share/examples/bitcoin.conf)

3. Bitcoin Core 설정파일 수정

### Bitcoin Core 설정파일 수정 내역

  1. _BitcoinCoreRPCAuth.py_ 활용 Bitcoin Core RPC 계정 생성

  2. RPC 서버 설정 (라인 75)

  ```
  server=1
  ```

  3. RPC 인증 설정 (라인 102)

  ```
  rpcauth=...
  ```

  4. 트랜잭션 인덱싱 설정 (라인 153)

  ```
  txindex=1
  ```

4. Bitcoin Core Re-index and Re-scan

- 1번만 수행할 것! 이후에 Bitcoin Core를 실행할 때에는 파라메터 없이 실행!

```bash
bitcoind -reindex -rescan
```

5. 테스트!

```bash
bitcoin-cli getblockheight 1
```
