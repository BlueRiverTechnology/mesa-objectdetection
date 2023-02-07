#converts unseperated rows of classes to comma and quotations seperated array
with open("1.txt", "r") as file:
    lines = file.readlines()
    modified_lines = ['"'+line.strip()+'",' for line in lines]
    with open("Labels.txt", "w") as file:
        file.writelines(modified_lines)
        file.close()