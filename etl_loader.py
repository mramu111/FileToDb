from zope.interface import Interface
from zope.component import (getUtility, getGlobalSiteManager,
                            ComponentLookupError)

from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from struct import *

_Base = declarative_base()


class User(_Base):
    __tablename__ = 'user'
    id = Column(Integer, Sequence('my_table_id_seq'), primary_key=True)
    name=Column(String(50), nullable=False)
    mail_id=Column(String(50), nullable=False)
    filedata= Column(BLOB)


class UserAddr(_Base):
    __tablename__='user_addr'
    id = Column(Integer, Sequence('my_table_id_seq'), primary_key=True)
    name =Column(String(50), nullable=False)
    addr1 =Column(String(50), nullable=False)
    addr2 = Column(String(50), nullable=False)



def _get_session():
    DB_NAME = 'sqlite:///BlobbingTest.db'
    db = create_engine(DB_NAME)
    db.echo = True
    _Base.metadata.create_all(db)
    Session = sessionmaker(bind=db)
    session = Session()
    return session

def create_session():
    session=_get_session() 
    return session
 

class IEtlApp(Interface):
    pass

class EtlApp(object):
    def __init__(self, config_path=None):
        self.config_path=config_path
    def start_etl(self, *etl_jobs):
        for etl_job in etl_jobs:
            try:
                etl_job.process()
            except Exception:
                pass
     

def register_app(app):
    gsm = getGlobalSiteManager()
    gsm.registerUtility(app, provided=IEtlApp)

def unregister_app(app):
    gsm = getGlobalSiteManager()
    gsm.unregisterUtility(app, provided=IEtlApp)

def get_etl_app(config_file_path=None):
    """
    """
    try:
        app = getUtility(IEtlApp)
    except ComponentLookupError:
        app = EtlApp(config_file_path)
        register_app(app)
    return app




class Conductor(object):
    def __init__(self, job):
        self.job=job
        
    def conduct_pipeline(self):
        row_iter=iter(self.job.reader)
    
        while True:
            try:
                context=row_iter.next()
                self.pipeline_stage_write(context)
            except StopIteration:
                break

    def pipeline_stage_write (self, context):
        try:
            self.job.writer=eval(self.job.writer)
            writer = self.job.writer(context
                                 )
            context = writer.main()
        except KeyboardInterrupt: #zc_question is this the best level for handling KeyboardInterrupt?
            raise
        return context




class EtlJob(object):
    def __init__(self, writer):
        self.writer=writer
        self.reader=self._make_reader()
        
    def process(self, cfg=None):
        conductor = Conductor(self)
        try:
            conductor.conduct_pipeline()
        except Exception:
            pass





class FileToDB(EtlJob):
    def file_name(self):
        return self.input_source_name
    def process(self, cfg=None):
        EtlJob.process(self)



class CsvToDb(FileToDB):
    def _make_reader(self):
        file_path='/home/ramu/sudo_code/sqlalchemy_code/hello.txt'
        reader = open(file_path, 'rU')
        return reader


class EtlWriter(object):
    def __init__(self):
        self.session=create_session()

    def main(self):
        self.populate()
        self.session.close()

class FileWriter(EtlWriter):
    def populate(self):
        self.context.out_data=self.context.transformed_data
    def commit(self):
        self.session.commit()

class RowLoader(EtlWriter):
    def __init__(self, context, cfg=None):
        self.session=create_session()
    

class JivaRowLoader(RowLoader):
    pass 

class UserLoader(JivaRowLoader):

    def populate(self):
        user=User()
        user.name='sample'
        user.mail_id='sample@mail.com'       
        user.filedata='hello\n ramu\n'
        self.session.add(user)
        self.session.flush()
        self.session.commit()


class UserAddrLoader(JivaRowLoader):
    def populate(self):
        useraddr=UserAddr()
        useraddr.name='sample'
        useraddr.addr1='ec1'       
        useraddr.addr2='ec2'
        self.session.add(useraddr)
        self.session.flush()
        self.session.commit()





if __name__=="__main__":
    app=get_etl_app()
    writer='UserLoader'
    user_job=CsvToDb(writer=writer)
    writer='UserAddrLoader'
    user_addr_job=CsvToDb(writer=writer)
    app.start_etl(user_job, user_addr_job


