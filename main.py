#!/usr/bin/python3

import click
import csv
import pymysql.cursors
import yaml


def gen_insert_sql(tp_name, columns, rows):
    wrap_columns = [f'`{c}`' for c in columns]
    value_format = '(%s)' % ', '.join(['%r' for _ in columns])
    values = [value_format % tuple(row.values()) for row in rows]

    sql = f'TRUNCATE TABLE {tp_name};\n\n'
    sql += f'INSERT INTO {tp_name} (%s) VALUES %s;' % (
        ', '.join(wrap_columns), ', \n'.join(values))
    sql = sql.replace('None', 'NULL')
    return sql


def export_to_csv(tp_name, columns, rows):
    with open(f'{tp_name}.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns)

        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_sql(name, content):
    with open(f'{name}.sql', 'w') as f:
        f.write(content)


with open("config.yml", 'r') as f:
    config = yaml.safe_load(f)


@click.group()
@click.option('--single-export', is_flag=True, help='Is exported to single file?')
@click.pass_context
def cli(ctx, single_export):
    ctx.ensure_object(dict)
    ctx.obj['SINGLE_EXPORT'] = single_export
    click.echo('Export to single file is %s.' %
               ('on' if single_export else 'off'))


@cli.command()
@click.pass_context
def gen_from_db(ctx):
    from_db = config.get('from_db', {})
    db_config = from_db.get('db', {})
    single_export_file_name = from_db.get(
        'single_export_file_name', 'tp_from_db')

    connection = pymysql.connect(
        host=db_config.get('host'),
        user=db_config.get('user'),
        password=db_config.get('password'),
        database=db_config.get('database'),
        charset=db_config.get('charset'),
        cursorclass=pymysql.cursors.DictCursor
    )
    tps_config = from_db.get('tps', [])
    if not tps_config:
        click.echo('No tps are configured.')
        return

    try:
        insert_sqls = []
        for tp in tps_config:
            name = tp.get('name')
            sql = tp.get('sql')
            export_csv = tp.get('export_csv', False)

            with connection.cursor() as cursor:
                cursor.execute(sql)

                columns = [field[0] for field in cursor.description]
                rows = cursor.fetchall()

                if export_csv:
                    export_to_csv(name, columns, rows)

                insert_sql = gen_insert_sql(name, columns, rows)

                if ctx.obj['SINGLE_EXPORT']:
                    insert_sqls.append(insert_sql)
                else:
                    write_sql(name, insert_sql)

        if ctx.obj['SINGLE_EXPORT']:
            write_sql(single_export_file_name, '\n\n'.join(insert_sqls))

    except Exception as e:
        print(e)
    finally:
        connection.close()

    click.echo('Done.')


@cli.command()
@click.pass_context
def gen_from_csv(ctx):
    from_csv = config.get('from_csv', {})
    tps = from_csv.get('tps', [])
    single_export_file_name = from_csv.get(
        'single_export_file_name', 'tp_from_csv')
    insert_sqls = []

    for tp in tps:
        name = tp.get('name')
        path = tp.get('path')
        columns = tp.get('columns', [])

        with open(path, 'r', newline='') as f:
            reader = csv.DictReader(f)
            if set(reader.fieldnames) != set(columns.keys()):
                click.echo(f'{path} header is not equal config columns.')
                continue
            rows = []
            for row in reader:
                for field in row:
                    field_type = columns.get(field)
                    if field_type == 'int':
                        row[field] = int(
                            row[field]) if row[field] != '' else None
                rows.append(row)
            insert_sql = gen_insert_sql(name, columns, rows)

            if ctx.obj['SINGLE_EXPORT']:
                insert_sqls.append(insert_sql)
            else:
                write_sql(name, insert_sql)

    if ctx.obj['SINGLE_EXPORT']:
        write_sql(single_export_file_name, '\n\n'.join(insert_sqls))

    click.echo('Done.')


if __name__ == '__main__':
    cli(obj={})
