# hotdeal_bot
Discord bot that tells you the hot deal
핫딜 게시글이 올라오는 게시판을 크롤링하여 실시간으로 메시지를 보내는 discord bot 입니다.

[디스코드 봇 설치하기](https://discord.com/oauth2/authorize?client_id=1225448505313857546&permissions=8&integration_type=0&scope=bot)

- 프로젝트 소개 및 주요 기능
    - 뽐뿌, 루리웹 등의 핫딜 게시판 크롤링하여 discord 메시지 전송
    - 키워드 설정 시 멘션으로 알림
    - 예시
        
    ![example](https://github.com/user-attachments/assets/bb2a33f7-1196-43fd-b165-de675ec3d8ef)


- 기술 스택 및 개발 상세 내용
    - 게시글 목록 수집 및 각 상품 정보 크롤링 - selenium 사용
    - 에러 발생 시 로깅 - slack 사용
    - message broker - kafka 사용
    - DB - postgresql 사용

![diagram](https://github.com/user-attachments/assets/192cca48-8d84-46a8-9e5b-79d25e3e5112)

---
### To do
- kafka 대신 redis queue 사용 (volume에 비해 불필요한 resource 낭비)
- keyword table 주기적 update
