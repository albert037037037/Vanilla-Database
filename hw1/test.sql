CREATE TABLE Professor{
    pid INTEGER,
    p_name VARCHAR(20),
    rank INTEGER,
    PRIMARY KEY (pid)
};

CREATE TABLE Student{
    s_id INTEGER,
    pid INTEGER,
    s_name VARCHAR(20),
    degree_program VARCHAR(20),
    PRIMARY KEY (s_id),
    FOREIGN KEY (pid) REFERENCES Professor ON DELETE CASCADE
};

CREATE TABLE Record{
    s_id INTEGER,
    project_num INTEGER,
    s_start DATE,
    s_stop DATE,
    paid REAL
    PRIMARY KEY (s_id, project_num),
    FOREIGN KEY (s_id) REFERENCES Student ON DELETE CASCADE,
    FOREIGN KEY (project_num) REFERENCES Project ON DELETE CASCADE
};

CREATE TABLE Project{
    project_num INTEGER,
    pid INTEGER,
    sponsor_name VARCHAR(20),
    starting_date DATE,
    ending_date DATE,
    budget REAL,
    PRIMARY KEY (project_num),
    FOREIGN KEY pid REFERENCES Professor ON DELETE CASCADE
};