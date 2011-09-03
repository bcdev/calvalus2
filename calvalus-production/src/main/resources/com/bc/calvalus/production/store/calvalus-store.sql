DROP TABLE person_course IF EXISTS;
DROP TABLE person IF EXISTS;
DROP TABLE course IF EXISTS;
DROP TABLE day_of_week IF EXISTS;


CREATE TABLE person
(
    id        INTEGER PRIMARY KEY,
    name      VARCHAR(256) NOT NULL
);


CREATE TABLE course
(
    id         INTEGER PRIMARY KEY,
    name       VARCHAR(256) NOT NULL,
    startTime  TIME,
    stopTime   TIME
);

CREATE TABLE person_course (
    id         INTEGER PRIMARY KEY,
    person_id  INTEGER NOT NULL,
    course_id  INTEGER NOT NULL,

    FOREIGN KEY (person_id) REFERENCES person (id),
    FOREIGN KEY (course_id) REFERENCES course (id)
);


CREATE TABLE day_of_week
(
    id         INTEGER PRIMARY KEY,
    name       VARCHAR(16) NOT NULL
);

INSERT INTO day_of_week VALUES(1, 'Montag');
INSERT INTO day_of_week VALUES(2, 'Dienstag');
INSERT INTO day_of_week VALUES(3, 'Mittwoch');
INSERT INTO day_of_week VALUES(4, 'Donnerstag');
INSERT INTO day_of_week VALUES(5, 'Freitag');
INSERT INTO day_of_week VALUES(6, 'Samstag');
INSERT INTO day_of_week VALUES(7, 'Sonntag');


