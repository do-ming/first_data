## git 올리기

Git 

—
git pull origin master 을 통해서 새롭게 바뀐 부분에 대해서 먼저 지정해줘야된다
—



git init (깃의 첫 시작) - 특정 폴더로부터 깃을 시작하겠다
(Touch sample.txt) => txt파일 만들기 샘플

현재상태 파악하기
git status

commit(message, snapshot) => 그상태를 기억하겠다

git add sample.txt (git에 add를 한다는 의미는 올리는 파일들을 설정하겠다)
그 후에 status를 하게 되면 이제 올리는 리스트는 알겠지만 commit을 메시지를 입력!

git commit -m “메시지” => 이걸 하게 되면 commit의 고유 메시지가 나오게 된다 (버전관리용)

(git commit이 안될경우)
git config —global user.email “alikerocknv@naver.com”
git config —global user.name “user.name”

git remote add origin 주소.

git push origin master



## crontab 관리하기

### 2.1 배치파일 작성할 장소 이동
터미널은 콘솔옆~

### 2.2 배치파일 만들자.
먼저 터미널창에서 쉘 파일을 만들자~ 
vi /home/dio/BAT/another_TB.sh (여기 안에다 nohup 치는거다!!!!)

### 2.3 배치파일 내용물 작성
쉘파일에 다음과 같이 적자~
Command + option + Iㅁ

nohup /usr/lib64/R/bin/Rscript /home/dio/RSC/another_test.R > /home/dio/BAT/LOG/another_TB_2019_12_17.log &

### 저장 후 exit
나가기 :wq

### 2.4 내가 원하는 시간대 돌리자~
터미널에서 crontab -l 쳐보자 
어떻게 나오는가?

crontab -e 쳐서 스케쥴 등록하자~
[분 시 일 월 요일(0~7)]
10 22 * * * /home/dio/BAT/another_TB_2019_12_17.sh
저장후 나오자

### 2.5 잘 생성됐는지 한번 보고 두번보자 마지막 처리만 하면 끝!!!
이거 무조건 해야된다!!
chmod 754 /home/dio/BAT/another_TB.sh


### 리눅스 실행중인 프로세스 죽이기

#1. 실행되고 있는 프로세스 이름 확인
ps -ef | grep [실행중인 프로세스 이름]  

#2 (kill -9 [pid]) 프로레스 죽이기
Kill `ps -df | grep kimwh3 | grep -v grep | awk ‘{print $2}’`




###  airflow 구축 및 수정

에어플로우에 대해서


서버
- 우리가 직접 개발하는 위치
- 배치를 돌리기 위한 환경은 다를것이다.


실행 (메듀사)
- 서버들이 모여있는 곳
- 배치, 모델링을 하는 곳


가상환경
- 환경정보를 포함하는 곳
- 

도커
- 가상환경
- 메듀사 실행 소스




Airflow

Dags
타스크로 구성되있다
Start_here은 시작이 되는지 안되는지를 보려고 하는거다
오른쪽 위에 schedule는 시간을 얘기하는 것이다.
task는 연관성이 없을때 구분을 짓는다. (나눠서 해야 각각 실행 가능하다)


빨간불이 켜져있으면 clear누르면 재시작 하겠다라는 의미이다.
보통은 파이참에서 코드를 작성할떄는 pull을 사용하면 좋다 (다른사람 수정)
dags는 id를 통해서 구분자를 갖는다,

retries=2 재시도를 2번하겠다라는 의미이다.
retry_delay : timedelta(minutes = 1) 재시도 텀 기간을 의미

Dag = 다으 아이디, schdule
Crontab.guru (한국시간 을 기준으로 해야된다)


Kubernetespodoperator =
이 안에서는 image를 부르고 스크립트를 실행한다
하지만 스크립트가 변하면 이미지를 다시 만들어야 된다.



Get from version control

Commend + k : 커밋
(Push = gitlab 에 대한 푸쉬)
Commend + shift + k 푸쉬



Docker build -t ~~~
(이건 에어플로우에 대한 푸쉬)

docker build -t registry.gslook.com/23090/rscmdocker .
docker push registry.gslook.com/23090/rscmdocker
