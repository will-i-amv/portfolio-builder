from flask_login import UserMixin

from portfolio_builder import db, login_manager


class User(UserMixin, db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    username = db.Column(
        db.String(25), 
        index=True, 
        unique=True,
        nullable=False
    )
    password = db.Column(db.String(257), nullable=False)

    def __repr__(self):
        return (f"<Username: {self.username}>")


@login_manager.user_loader
def load_user(id):
    return User.query.get(int(id))
