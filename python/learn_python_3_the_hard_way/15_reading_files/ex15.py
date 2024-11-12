from sys import argv

script, filename = argv

text = open(filename, 'r')

print(f"Here's your file {filename}:")
print(text.read())
text.close()

print("Type the filename again:")
file_again = input("> ")

txt_again = open(file_again)

print(txt_again.read())
txt_again.close()
