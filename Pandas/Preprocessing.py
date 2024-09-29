import pandas as pd
from datetime import datetime

class DataCleaner:
    def __init__(self, df):
        self.df = df
        self.eng_to_rus = {
            'a': 'а', 'k': 'к', 'r': 'р', 'i': 'и', 'l': 'л', 'o': 'о', 'v': 'в', 't': 'т', 'u': 'у', 'e': 'е',
            'sh': 'ш', '\'': 'ь', 'ja': 'я', 's': 'с', 'n': 'н'
        }
        self.rus_to_eng = {
            'а': 'a', 'в': 'b', 'е': 'e', 'к': 'k', 'м': 'm', 'н': 'h', 'о': 'o', 'р': 'p', 'с': 'c', 'т': 't',
            'у': 'y', 'х': 'x'
        }
        self.mail_removal = ['gmail', '.mail', 'example', 'yandex', '.ru', '.com', '.net']

    # def replace_english_letters(self, text):
    #     """
    #     Имя, вспомогательный метод
    #     """
    #     text = str(text)
    #     result = []
    #     i = 0
    #     while i < len(text):
    #         if i + 1 < len(text) and text[i:i + 2] in self.eng_to_rus:
    #             result.append(self.eng_to_rus[text[i:i + 2]])
    #             i += 2
    #         else:
    #             result.append(self.eng_to_rus.get(text[i], text[i]))
    #             i += 1
    #     return ''.join(result)

    def replace_english_letters_fast(self, text):
        """
        Имя, вспомогательный метод. Заменяем английские симводы длиной 1
        """
        return ''.join(self.eng_to_rus.get(char, char) for char in text)

    def clean_name_assist(self, text):
        """
        Имя, вспомогательный метод. Убираем цифры
        """
        text = str(text)
        return ''.join([char for char in text if not char.isdigit()])

    def clean_name(self):
        """
        Имя
        """
        self.df['full_name'] = self.df['full_name'].str.lower()
        self.df['full_name'] = self.df['full_name'].str.replace('\n', '_')
        self.df['full_name'] = self.df['full_name'].str.replace('-', '')
        self.df['full_name'] = self.df['full_name'].str.replace(' оглы', '')
        self.df['full_name'] = self.df['full_name'].str.replace(' углы', '')
        self.df['full_name'] = self.df['full_name'].str.replace(' угли', '')
        self.df['full_name'] = self.df['full_name'].str.replace(' огли', '')
        self.df['full_name'] = self.df['full_name'].str.replace(' нет', '')
        self.df['full_name'] = self.df['full_name'].str.replace(' отсутствует', '')
        self.df['corrected_full_name'] = self.df['full_name'].apply(self.replace_english_letters_fast)
        self.df['corrected_full_name'] = self.df['corrected_full_name'].apply(self.clean_name_assist)

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

    def remove_trailing_numbers(self, text):
        """
        Removes trailing numbers from the given string
        """
        return re.sub(r'\d+$', '', str(text))

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
        self.df['corrected_email'] = self.df['corrected_email'].apply(self.remove_trailing_numbers)

    def clean_adress(self):
        """
        Очистка адресов
        """
        self.df['address'] = self.df['address'].str.lower()
        self.df['address'] = self.df['address'].str.replace('-', '')
        self.df['address'] = self.df['address'].str.replace('\n', '0')
        self.df['address'] = self.df['address'].str.replace('_', '')
        self.df['address'] = self.df['address'].str.replace('село ', '')
        self.df['address'] = self.df['address'].str.replace('поселок ', '')
        self.df['address'] = self.df['address'].str.replace('город ', '')
        self.df['address'] = self.df['address'].str.replace('ст. ', '')
        self.df['address'] = self.df['address'].str.replace('клх ', '')
        self.df['address'] = self.df['address'].str.replace('бул. ', '')
        self.df['address'] = self.df['address'].str.replace('с. ', '')
        self.df['address'] = self.df['address'].str.replace('с. ', '')
        self.df['address'] = self.df['address'].str.replace('г. ', '')
        self.df['address'] = self.df['address'].str.replace('к. ', '')
        self.df['address'] = self.df['address'].str.replace('д. ', '')
        self.df['address'] = self.df['address'].str.replace('деревня ', '')
        self.df['address'] = self.df['address'].str.replace('квартира ', '')
        self.df['address'] = self.df['address'].str.replace('булю ', '')
        self.df['address'] = self.df['address'].str.replace('строение ', '')
        self.df['address'] = self.df['address'].str.replace('стр. ', '')
        self.df['address'] = self.df['address'].str.replace('ул. ', '')
        self.df['address'] = self.df['address'].str.replace('улица ', '')
        self.df['address'] = self.df['address'].str.replace('пер. ', '')
        self.df['address'] = self.df['address'].str.replace('пер.', '')
        self.df['address'] = self.df['address'].str.replace('ш. ', '')
        self.df['address'] = self.df['address'].str.replace('шоссе ', '')
        self.df['address'] = self.df['address'].str.replace('алл. ', '')
        self.df['address'] = self.df['address'].str.replace('пр. ', '')
        self.df['address'] = self.df['address'].str.replace('наб. ', '')
        self.df['address'] = self.df['address'].str.replace('уб. ', '')
        self.df['address'] = self.df['address'].str.replace('п. ', '')
        self.df['address'] = self.df['address'].str.replace('дом ', '')
        self.df['address'] = self.df['address'].str.replace('р. ', '')
        self.df['address'] = self.df['address'].str.replace('.', '')
        self.df['address'] = self.df['address'].str.replace(',', '')
        self.df['address'] = self.df['address'].str.replace(' ', '')
        self.df['address'] = self.df['address'].str.replace('-', '')
        self.df['address'] = self.df['address'].str.replace('/', '')



main1 = pd.read_csv(r'D:\main1.csv')
Cleaner1 = DataCleaner(main1)
Cleaner1.clean_name()
Cleaner1.clean_email()
Cleaner1.clean_phone_number()
main1[['last_name', 'first_name', 'middle_name']] = main1['corrected_full_name'].str.split(' ', n=2, expand=True)
main1.drop(columns=(['birthdate', 'full_name', 'email', 'corrected_full_name']), inplace=True)


main2 = pd.read_csv(r'D:\main2.csv')
main2['full_name'] = main2['first_name'] + ' ' + main2['middle_name'] + ' ' + main2['last_name']
main2.drop(columns=(['first_name', 'middle_name', 'last_name']), inplace=True)
Cleaner2 = DataCleaner(main2)
Cleaner2.clean_name()
Cleaner2.clean_phone_number()
main2[['first_name', 'middle_name', 'last_name']] = main2['corrected_full_name'].str.split(' ', n=2, expand=True)
main2.drop(columns=(['birthdate', 'full_name', 'corrected_full_name']), inplace=True)


main3 = pd.read_csv(r'D:\main3.csv')
main3.rename(columns={"name": "full_name"}, inplace=True)
Cleaner3 = DataCleaner(main3)
Cleaner3.clean_name()
Cleaner3.clean_email()
main3[['first_name', 'last_name']] = main3['corrected_full_name'].str.split(' ', n=1, expand=True)
main3.drop(columns=(['birthdate', 'email', 'full_name', 'corrected_full_name']), inplace=True)
