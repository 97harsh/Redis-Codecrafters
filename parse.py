
class RESPParser:
    @staticmethod
    def process(string):
        print(string)
        # breakpoint()
        if string[0:1]==b"+":
            result = RESPParser.process_simple_string(string)
        elif string[0:1]==b"*":
            result = RESPParser.process_arrays(string)
        else:
            return string
        return result

    @staticmethod
    def process_arrays(input):
        """
        Returns a list of items received as input string
        """
        input_list = input.split(b"\r\n")
        # Length list, length item, item, length item, ...'
        # Get every other element
        return input_list[::2][1:]

    @staticmethod
    def process_simple_string(input):
        """
        Extracts the string item from the input
        """
        # +PONG\r\n
        input_processed = input[1:-2]
        return input_processed
    
    @staticmethod
    def convert_string_to_resp(input):
        input_processed = b"+"+RESPParser.convert_string_binary(input)+\
            b"\r\n"
        return input_processed
    
    @staticmethod
    def convert_list_to_resp(input):
        length = RESPParser.convert_string_binary(len(input))
        output = b"*"+length
        for item in input:
            len_str = RESPParser.convert_string_binary(len(item))
            output += "$"+len_str+b"\r\n"+\
                RESPParser.convert_string_binary(item)+\
                    b"\r\n"
        return output

    @staticmethod
    def convert_string_binary(input):
        if isinstance(input,bytes):
            return input
        elif isinstance(input,str):
            return input.encode("UTF-8")
        elif isinstance(input,int):
            return str(input).encode('UTF-8')
        else:
            raise ValueError("Unexpected input format")