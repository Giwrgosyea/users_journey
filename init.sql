CREATE TABLE user_browsing(
user_id varchar(50) NOT NULL , 
session_id varchar(50) NOT NULL, 
browse_timestamp timestamp NOT NULL,
page varchar(50) NOT NULL,
constraint pk_test1 primary key (user_id, session_id, browse_timestamp));


CREATE TABLE user_transactions(
user_id varchar(50) NOT NULL, 
session_id varchar(50) NOT NULL, 
transaction_timestamp timestamp NOT NULL,
transaction varchar(50) NOT NULL,
constraint pk_test2 primary key (user_id, session_id, transaction_timestamp));


CREATE TABLE user_total_journey(
user_id varchar(50) NOT NULL, 
session_id varchar(50) NOT NULL, 
browse_timestamp timestamp NOT NULL,
page varchar(50) NOT NULL,
transaction_timestamp timestamp NOT NULL,
transaction varchar(50) NOT NULL,
constraint pk_test3 primary key (user_id, session_id, browse_timestamp));

CREATE TABLE user_journey_to_buy_visited(
user_id varchar(50) NOT NULL, 
session_id varchar(50) NOT NULL, 
browse_timestamp timestamp NOT NULL,
page varchar(50) NOT NULL,
transaction_timestamp timestamp NOT NULL,
transaction varchar(50) NOT NULL,
constraint pk_test4 primary key (user_id, session_id, browse_timestamp));

CREATE TABLE time_spend_per_ses(
user_id varchar(50) NOT NULL, 
session_id varchar(50) NOT NULL, 
browse_timestamp timestamp NOT NULL,
page varchar(50) NOT NULL,
transaction_timestamp timestamp NOT NULL,
transaction varchar(50) NOT NULL,
date_diff_min float NOT NULL,
constraint pk_test5 primary key (user_id, session_id, browse_timestamp));

CREATE TABLE end_transactions(
user_id varchar(50) NOT NULL, 
session_id varchar(50) NOT NULL, 
browse_timestamp timestamp NOT NULL,
page varchar(50) NOT NULL,
transaction_timestamp timestamp NOT NULL,
transaction varchar(50) NOT NULL,
date_diff_last_page float NOT NULL,
constraint pk_test6 primary key (user_id, session_id, browse_timestamp));