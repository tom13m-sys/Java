import random
from enum import Enum

class CardNumbers(Enum):
    ONE = 1
    TWO = 2
    THREE = 3

class CardShapes(Enum):
    DIAMOND = 1
    SQUIGGLE = 2
    OVAL = 3

class CardShadings(Enum):
    SOLID = 1
    STRIPED = 2
    OPEN = 3

class CardColors(Enum):
    RED = 1
    GREEN = 2
    PURPLE = 3

class Card:
    def __init__(self, number: CardNumbers, shape: CardShapes, shading: CardShadings, color: CardColors):
        self.number = number
        self.shape = shape
        self.shading = shading
        self.color = color

    def __str__(self):
        return f"[{self.number.name}, {self.shape.name}, {self.shading.name}, {self.color.name}]"


class Deck:
    def __init__(self):
        self.cards = []

        # add all possible card combinations
        for number in CardNumbers:
            for shape in CardShapes:
                for shading in CardShadings:
                    for color in CardColors:
                        self.cards.append(Card(number, shape, shading, color))
        
        # shuffle the deck
        random.shuffle(self.cards)

    def is_empty(self) -> bool:
        return len(self.cards) < 3

    def draw_three(self) -> list[Card]:
        if self.is_empty():
            raise Exception("Not enough cards left in the deck.")

        return [self.cards.pop(0), self.cards.pop(0), self.cards.pop(0)]

    def size(self) -> int:
        return len(self.cards)


class SetValidator:
    
    @staticmethod
    def __all_same_or_all_different(f1, f2, f3) -> bool:
        # all three are identical
        if f1 == f2 and f2 == f3:
            return True
        # all three are different from one another
        if f1 != f2 and f2 != f3 and f1 != f3:
            return True
        # all other options return false
        return False

    @classmethod
    def is_set(cls, three_cards: list[Card]) -> bool:
        if not three_cards or len(three_cards) != 3:
            return False
            
        c1, c2, c3 = three_cards

        # check games rules for finding a set
        return (
            cls.__all_same_or_all_different(c1.number, c2.number, c3.number) and # same number on all 3 cards
            cls.__all_same_or_all_different(c1.shape, c2.shape, c3.shape) and # same shape on all 3 cards
            cls.__all_same_or_all_different(c1.shading, c2.shading, c3.shading) and # same shade on all 3 cards
            cls.__all_same_or_all_different(c1.color, c2.color, c3.color) # same color on all 3 cards
        )


if __name__ == "__main__":

    print("Initializing the deck...\n")
    deck = Deck()
    draw_counter = 1
    set_found = False

    while not deck.is_empty():
        current_draw = deck.draw_three()
        
        print(f"Draw #{draw_counter} (Remaining Cards: {deck.size()}):")
        for card in current_draw:
            print(f"{card}")

        if SetValidator.is_set(current_draw):
            print("Set found!\n")
            set_found = True
            break
        else:
            print("Not a set...\n")
            
        draw_counter += 1

    if not set_found:
        print("Went through all the deck. No set was found.\n")
