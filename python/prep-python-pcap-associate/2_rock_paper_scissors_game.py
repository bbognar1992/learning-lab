import random


class Player(object):

    def __init__(self):
        self.points = 0

    def move(self):
        pass


class HumanPlayer(Player):
    def move(self):
        while True:
            m = input("Rock, Paper, Scissors [r,p,s]")
            if m in ["r", "p", "c"]:
                return m


class ComputerPlayer(Player):
    def move(self):
        m = random.choice(["r", "p", "s"])
        return m


class Game(object):
    # r = rock, p = paper, s = scissors
    OUTCOMES = {('s', 's'): 0, ('p', 'p'): 0, ('r', 'r'): 0,  # tie
                ('r', 'p'): -1, ('p', 's'): -1, ('s', 'r'): -1,
                # human player loses
                ('p', 'r'): 1, ('s', 'p'): 1,
                ('r', 's'): 1}  # human player wins

    def __init__(self, n_rounds: int = 0):
        self.n_rounds = n_rounds
        self.human = HumanPlayer()
        self.computer = ComputerPlayer()

    def play(self):
        for i in range(self.n_rounds):
            self.play_round()
        self.summarise()

    def play_round(self):
        c_human = self.human.move()
        c_computer = self.computer.move()
        print(f"You: {c_human} | Computer: {c_computer}")
        self.evaluate(c_human, c_computer)

    def evaluate(self, c_human, c_computer):
        outcome = self.OUTCOMES[(c_human, c_computer)]
        if outcome == 1:
            print('You won this round!\n')
            self.human.points += 1
        elif outcome == -1:
            print('You lost this round!\n')
            self.computer.points += 1
        else:
            print('This round is a tie\n')
        pass

    def summarise(self):
        print('[Game summary] Your points:', self.human.points,
              ' | Computer points:', self.computer.points)
        if self.human.points > self.computer.points:
            print("You won.")
        elif self.human.points < self.computer.points:
            print("Computer won.")
        else:
            print("It was a tie.")


if __name__ == '__main__':
    print("--- Rock-Paper-Scissors Game ---")
    round_count = int(input('How many rounds would you like to play? '))
    Game(round_count).play()
