create table if not exists %(table)s (
  id          bigserial    PRIMARY KEY,
  enqueued_at timestamptz  NOT NULL DEFAULT current_timestamp,
  dequeued_at timestamptz,
  expected_at timestamptz,
  schedule_at timestamptz,
  completed_at timestamptz,
  error       text,
  fn_name     text,
  unique_key  text,
  q_name      text         NOT NULL CHECK (length(q_name) > 0),
  data        json         NOT NULL
);

create unique index if not exists unique_key_index on %(table)s (unique_key) where unique_key is not null;

create index if not exists priority_idx_%(name)s on %(table)s
      (schedule_at nulls first, expected_at nulls last, q_name)
    where dequeued_at is null
          and q_name = '%(name)s';

create index if not exists priority_idx_no_%(name)s on %(table)s
    (schedule_at nulls first, expected_at nulls last, q_name)
    where dequeued_at is null
          and q_name != '%(name)s';

alter table %(table)s add column if not exists completed_at timestamptz;
alter table %(table)s add column if not exists fn_name text;
drop function if exists pq_notify() cascade;

create function pq_notify() returns trigger as $$ begin
  perform pg_notify(
    case length(new.q_name) > 63
      when true then 'pq_' || md5(new.q_name)
      else new.q_name
    end,
    ''
  );
  return null;
end $$ language plpgsql;

create trigger pq_insert
after insert on %(table)s
for each row
execute procedure pq_notify();
