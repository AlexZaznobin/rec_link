import pandas as pd
from datetime import datetime

class DataCleaner:
    def __init__(self, df):
        self.df = df
        self.eng_to_rus = {
            'a': 'а', 'k': 'к', 'r': 'р', 'i': 'и', 'l':'л', 'o':'о', 'v':'в', 't':'т', 'u':'у', 'e':'е', 
            'sh':'ш', '\'':'ь', 'ja':'я', 's':'с', 'n':'н'
        }
        self.rus_to_eng = {
            'а': 'a', 'в': 'b', 'е': 'e', 'к': 'k', 'м': 'm', 'н': 'h', 'о': 'o', 'р': 'p', 'с': 'c', 'т': 't', 
            'у': 'y', 'х': 'x'
        }
        self.mail_removal = ['gmail', 'mail', 'example', 'yandex', '.ru', '.com', '.net']

#     def replace_english_letters(self, text):
#         """
#         Имя, вспомогательный метод. Слишком много времени занимает (2 минуты) и не нужен как таковой
#         """
#         text = str(text)  
#         result = []
#         i = 0
#         while i < len(text):
#             if i + 1 < len(text) and text[i:i+2] in self.eng_to_rus:
#                 result.append(self.eng_to_rus[text[i:i+2]]) 
#                 i += 2
#             else:
#                 result.append(self.eng_to_rus.get(text[i], text[i]))
#                 i += 1
#         return ''.join(result)
    
    def clean_name(self):
        """
        Имя
        """
        self.df['full_name'] = self.df['full_name'].str.lower()
        self.df['full_name'] = self.df['full_name'].str.replace('\n', '_')
        self.df['full_name'] = self.df['full_name'].str.replace('-', '')
        self.df['full_name'] = self.df['full_name'].str.replace(' оглы', '')
        self.df['full_name'] = self.df['full_name'].str.replace(' углы', '')
        # self.df['corrected_full_name'] = self.df['full_name'].apply(self.replace_english_letters)
    
    def phone_number_missing_plus(self, phone):
        """
        Номер телефона, вспомогательная функция
        """

        if phone.startswith('7'):
            return '8' + phone[1:]
        return phone
    
    def clean_phone_number(self):
        """
        Номер телефона
        """
        self.df['phone'] = self.df['phone'].astype(str).str.replace(' ', '')
        self.df['phone'] = self.df['phone'].str.replace('(', '')
        self.df['phone'] = self.df['phone'].str.replace(')', '')
        self.df['phone'] = self.df['phone'].str.replace('-', '')
        self.df['phone'] = self.df['phone'].str.replace('+7', '8')
        
        self.df['phone'] = self.df['phone'].apply(self.phone_number_missing_plus)

    def replace_russian_letters_mail(self, text):
        """
        Почта, вспомогательный метод
        """
        return ''.join(self.rus_to_eng.get(char, char) for char in str(text))

    def remove_after_at(self, email):
        """
        Почта, вспомогательный метод
        """
        email = str(email)
        if '@' in email:
            return email.split('@')[0]
        for fragment in self.mail_removal:
            if fragment in email:
                return email.split(fragment)[0]
        return email
    
    def clean_email(self):
        """
        Почта
        """
        self.df['email'] = self.df['email'].apply(self.replace_russian_letters_mail)
        self.df['corrected_email'] = self.df['email'].apply(self.remove_after_at)

    def clean_birthdates_assist(self, birthdate):
        """
        Дни рождения, вспомогательный метод
        """
        current_year = datetime.now().year
        
        if len(birthdate) < 8:
            return birthdate

        if len(birthdate) == 8:
            year = birthdate[:2]
            
            if year[0] != '0':                           # 89 to 1989
                corrected_year = '19' + year  
                return corrected_year + birthdate[2:]
            else:                                        # 04 to 2004
                corrected_year = '20' + year  
                return corrected_year + birthdate[2:]
            return birthdate 

        if len(birthdate) == 9:
            year = birthdate[:3] 

            if year[0] == '9':
                corrected_year = '1' + year  
                return corrected_year + birthdate[3:]
            return birthdate 
            
        if len(birthdate) == 10:
            year = birthdate[:4]
            
            if birthdate.startswith('-'):
                return '1' + birthdate[1:]

            elif year.startswith(('29', '39', '49', '59', '69', '79', '89', '99')): # 2991 to 1991
                corrected_year = '19' + year[2:]  
                return corrected_year + birthdate[4:]
                
            elif year[0] == '2' and year[1] != '0':           # 2701 to 2001 !!!! нужно улучшить (не 2719 to 2019)
                corrected_year = '20' + year[2:]  
                return corrected_year + birthdate[4:]
                
            elif year[0] == '1' and year[1] != '9':           # 1039 to 1939
                corrected_year = '19' + year[2:] 

                if int(corrected_year) < current_year - 100:  # 1004 to 2004 (Если возраст больше 100 лет)
                    corrected_year = '20' + year[2:]
                return corrected_year + birthdate[4:]         # '3004' to '2004'
                
            elif year[0] > '2' and year[1] == '0':            # 3004 to 2004
                corrected_year = '20' + year[2:]  
                return corrected_year + birthdate[4:] 
        
            elif year[0] > '2' and year[1] > '0':             # 8169 to 1969 or 8104 to 2004 (if 2 last > or < 24)
                current_year = str(current_year)

                if int(year[0]+year[1]) > int(current_year[2:]):
                    corrected_year = '19' + year[2:]  
                else:
                    corrected_year = '20' + year[2:]  

                return corrected_year + birthdate[4:]    
        return birthdate
    
    def clean_birthdates(self):
        """
        Дни рождения
        """
        self.df['corrected_birthdate'] = self.df['birthdate'].apply(self.clean_birthdates_assist)


cleaner = DataCleaner(main1)
