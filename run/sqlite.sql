create table job (
    id integer primary key,
    action text,
    process integer,
    status text,
    target text,
    location text,
    instanceid text
);

create table jobdetail (
    id integer primary key,
    tasklist text,
    donelist text
);
