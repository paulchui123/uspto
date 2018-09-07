import os
import SQLProcessor

# This funtion accepts a line from the class text file and
# parses it and returns a dictionary to build an sql query string
def return_classification_array(line):

    # Build a class dictionary
    class_dictionary = {
        "table_name" : "uspto.CLASSIFICATION",
        "Class" : line[0:3],
        "Subclass" : line[3:9],
        "Indent" : line[9:11],
        "SubclsSqsNum" : line[11:15],
        "NextHigherSub" : line[15:21],
        "Title" : line[21:len(line)+1].strip()[0:140]
    }

    # Return the class dictionary
    return class_dictionary

if __name__ == "__main__":

    # Define variables
    working_diretory = os.getcwd()
    class_text_file_path = working_diretory + "/CLS/ctaf1204.txt"
    class_data_file = open(class_text_file_path,'r')
    line = True
    number_of_records = 1

    # Loop through every line in file
    while line:

        # Read one line from class file
        line = class_data_file.readline()
        # Returns a array of items parsed from the line
        class_item = return_classification_array(line)

        print class_item

        number_of_records += 1

    # Close connections and read file
    class_data_file.close()
    processor.close()
    print '**********\nCongratulations!{0} records have been inserted into the database successfully!\n**********'.format(number_of_records)
