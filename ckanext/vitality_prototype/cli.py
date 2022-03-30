import click

def get_commands():
    return[vitality]

@click.group()
def vitality():
    '''For use with the Vitality commands'''
    pass

@vitality.command()
def seed_all():
    click.echo('This is working now')
    return