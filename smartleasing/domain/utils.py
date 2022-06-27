class Utils:
    @classmethod
    def translate(cls, text):
        test = text.replace('\u011f', 'g').replace('\u011e', 'G').replace('\u0131', 'i').replace('\u0130', 'I').replace(
            '\u00f6', 'o').replace('\u00d6', 'O').replace('\u00fc', 'u').replace('\u00dc', 'U').replace('\u015f',
                                                                                                        's').replace(
            '\u015e', 'S').replace('\u00e7', 'c').replace('\u00c7', 'C')
        return test
