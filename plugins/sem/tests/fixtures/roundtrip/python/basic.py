from dataclasses import dataclass


API_VERSION = "v1"


@dataclass
class User:
    id: str
    name: str


def greet(user: User) -> str:
    return f"Hello, {user.name}!"


if __name__ == "__main__":
    print(greet(User(id="1", name="Ada")))
