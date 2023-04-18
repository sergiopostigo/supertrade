"""
Data Formatter
Utilities

This script contains functions used in multiple scripts in the Data Formatter stage.

"""

import os

def get_headings():
    """Return the headings to focus on

    Get the list with the first two digits of the headings to focus on.
    07XXXXXXXX: Hortalizas, plantas, raíces y tubérculos alimenticios
    08XXXXXXXX: Frutas y frutos comestibles; cortezas de agrios (cítricos), melones o sandías.

    Returns:
        headings (list): List with the first two digits of the headings to focus on
    """
    headings = ['07', '08']
    return headings

def delete_file_request(filename):
    """Remove a file in the same folder where this function is invoked

        Remove a file in the current folder and ask for confirmation to delete

        Args:
            filename (string): Name of the file to delete
        """

    instruction = 'Remove {}?'.format(filename)
    user_input = input(instruction + " (yes/no): ").lower()
    if user_input == 'yes' or user_input == 'y':
        os.remove('./' + filename)
        print('File {} deleted'.format(filename))
    elif user_input == 'no' or user_input == 'n':
        print('Deletion of file {} cancelled'.format(filename))
    else:
        print("Invalid input. Please enter 'yes' or 'no'.")