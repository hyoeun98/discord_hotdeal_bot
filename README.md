# hotdeal_bot

핫딜 게시글이 올라오는 게시판을 크롤링하여 실시간으로 메시지를 보내는 discord bot 입니다.

![Discord Invite Link Validation](https://github.com/hyoeun98/hotdeal_bot/actions/workflows/discord_invite_link_validation.yml/badge.svg)

[디스코드 봇 설치하기(25.05.19)](https://discord.com/oauth2/authorize?client_id=1346055722676260985)

- 프로젝트 소개 및 주요 기능
    - 뽐뿌, 루리웹 등의 핫딜 게시판 크롤링하여 discord 메시지 전송
    - 키워드 설정 시 멘션으로 알림
    - 멘션으로 알린 메세지는 thread에 따로 모아둠
    - 단시간에 반응이 뜨거운 게시글(조회수, 댓글 등) 알림
      
    ![example_message](https://github.com/user-attachments/assets/66c59425-f8ad-494f-a691-344d876a2ba0)
  ![example_thread_message](https://github.com/user-attachments/assets/915f94d7-c228-40c5-ad00-ab927d971c03)



- 기술 스택 및 개발 상세 내용
    - 게시글 목록 수집 및 각 상품 정보 크롤링 - selenium 사용
    - 에러 발생 시 로깅 - slack 사용
    - message broker - ~aws SQS~ aws SNS 사용(필요없는 polling이 너무 빈번해 교체)
    - DB - postgresql 사용
    - Hash tag 생성 - ChatGPT-4.1 nano

![제목 없음-2025-04-11-1540](https://github.com/user-attachments/assets/baeaa592-1f0a-40f2-b198-9515f29d4535)

---
### To do
- keyword table 주기적 update
- ~cloud화~
  - ~data lake : postgreSQL -> s3로 대체~ Lightsail postgreSQL 사용
  - ~작업 큐 : kafka -> SQS로 대체~ SQS 대체 완료
  - ~crawler : ec2 or fargate~ Lambda 대체 완료
  - ~transform, message send : lambda + ec2~ Lightsail 대체 완료
- ~item 대분류 : chatgpt 4o mini or gemini 1.5 Flash-8B 사용~ ChatGPT 4.1 nano 사용
- item 분류 시 대표적인 class 추리기
- ~중요한 메세지를 모아두는 thread 생성~
- scan 시 댓글, 조회수 등을 기준으로 인기 게시글 선정하여 알리기
