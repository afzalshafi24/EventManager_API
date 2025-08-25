from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define the database URL
DATABASE_URL = "postgresql+psycopg2://shafiaf:1234@localhost/test_db"

# Create a new SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Define a base class for declarative models
Base = declarative_base()

# Define a sample model
class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    email = Column(String, nullable=False)

# Create the table in the database
Base.metadata.create_all(engine)

# Create a new session
Session = sessionmaker(bind=engine)
session = Session()

# Create a new user instance
new_user = User(name="Afzal", email="GANGGANG@example.com")

# Add the new user to the session
session.add(new_user)

# Commit the session to save the user to the database
session.commit()

# Close the session
session.close()

print("User added successfully!")

# Close the session
session.close()

print("User added successfully!")