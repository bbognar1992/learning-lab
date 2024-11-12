import os
import uuid


class Page(object):
    FILE_PATH = "tasks.txt"

    @classmethod
    def save_tasks(cls, tasks: dict) -> None:
        with open(cls.FILE_PATH, "w") as f:
            list_tasks = []
            for t_id, t_values in tasks.items():
                list_tasks.append(f"{t_id} | {t_values[0]} | {t_values[1]}")
            f.write("\n".join(list_tasks) + "\n")

    @classmethod
    def read_tasks(cls) -> dict:
        tasks = dict()
        if os.path.exists(cls.FILE_PATH):
            with open(cls.FILE_PATH, "r") as f:
                for line in f.read().split("\n"):
                    if line:
                        task = line.split(" | ")
                        tasks[task[0]] = task[1:]
        return tasks

    @classmethod
    def print_tasks(cls, tasks: dict):
        for t_id, t_values in tasks.items():
            print(f"{t_id} | {t_values[0]} | {t_values[1]}")

    @classmethod
    def show(cls) -> None:
        pass

    @classmethod
    def choice(cls) -> int:
        return 0


class MainMenu(Page):
    @classmethod
    def show(cls) -> None:
        print("== TODO LIST ==")
        print("[1] Show tasks")
        print("[2] Add task")
        print("[3] Complete task")
        print("[4] Exit\n")

    @classmethod
    def choice(cls) -> int:
        try:
            choice = int(input("Your choice:"))
            if choice not in range(1, 5):
                raise ValueError("Not in range")
        except ValueError as e:
            print(e)
            return 0
        else:
            return choice


class ShowTasks(Page):

    @classmethod
    def show(cls) -> None:
        print("\n[YOUR TASKS]")
        tasks = cls.read_tasks()
        if tasks:
            cls.print_tasks(tasks)
        else:
            print("Empty list\n")


class AddTask(Page):

    @classmethod
    def show(cls) -> None:
        print("[ADD TASK]")

    @classmethod
    def choice(cls) -> int:
        try:
            t_id = str(uuid.uuid4())
            t_name = input("What is the task?")
            t_deadline = input("What is deadline?")
            accepted_dl = ["today", "tomorrow"]
            if t_deadline not in accepted_dl:
                raise ValueError(f"{t_deadline} not in {accepted_dl}")

            tasks = cls.read_tasks()
            tasks[t_id] = [t_name, t_deadline]
            cls.save_tasks(tasks)

        except Exception as e:
            print(e)
            return 0
        else:
            return 1


class CompleteTask(ShowTasks):
    @classmethod
    def choice(cls) -> int:
        try:
            tasks = cls.read_tasks()
            if tasks:
                t_id = input("Enter id to complete")
                del tasks[t_id]
                cls.save_tasks(tasks)
        except Exception as e:
            print(e)
            return 0
        else:
            return 1


class Exit(Page):
    @classmethod
    def show(cls) -> None:
        exit(0)


if __name__ == "__main__":
    pages = {
        0: MainMenu,
        1: ShowTasks,
        2: AddTask,
        3: CompleteTask,
        4: Exit
    }
    selected_page = 0
    while True:
        pages[selected_page].show()
        selected_page = pages[selected_page].choice()
