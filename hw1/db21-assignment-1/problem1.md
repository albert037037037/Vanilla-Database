# Problem 1 - ER Models, Relational Models, Normal Forms

Please consider the following senerio and present your models for the requirements.

## Senerio

Let's say we want to design a database to manage the relationships between projects, profressors, and students.

- Each professor has a id, a name, a rank.
- Each project has a project number, a sponsor name (e.g. MOST), a starting date, an ending date, and a budget.
- Each student has a student id, a name, and a degree program (Bachelor, Master or PhD).
- Each project is managed by a professor. It is possible that a professor manages multiple projects.
- Each student is supervised by a professor. It is possible that a professor supervises multiple students.
- A student is able to work in a project. It is possible that a student participates in multiple projects.
- It should record when a student starts working in a project, when he/she stops, and how much he/she is paid per month.

## Requirements

1. Please design an ER model and draw an ER diagram for this senerio. (30 points)
2. Please design a relational model for this senerio with the following constraints. (30 points)
  - You should use SQL `CREATE TABLE` to present your schema.
  - You should `identify primary keys and foreign keys relationsips` in your SQLs.
  - You can only use [data types in PostgreSQL][1].
  - The relations should at least follow `the 3rd normal form`.

[1]: https://www.postgresql.org/docs/13/datatype.html
