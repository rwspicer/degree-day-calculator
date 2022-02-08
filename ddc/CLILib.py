"""
CLI Lib
-------

Simple python cli wrapper

Rawser Spicer

taken from utilitools v0.1.0

version: 1.0.0
Split from imiq-database python utilities: 2017-08-18

"""
import sys

class CLILibTypeError(Exception):
    """CLILib type error, raised if type is wrong for a provided flag
    """
    pass
    
class CLILibMandatoryError(Exception):
    """CLILib mandatory flag error, raised if all of the mandatory flags 
    are not present
    """
    pass
    
class CLILibHelpRequestedError(Exception):
    """CLILib help requested error, raised if -h or --help is provied in the 
    lsit of flags
    """
    pass

class CLI (object):
    """ This class will act as a simple CLI utility """
    
    def __init__ (self, mandatory, optional = [], types = {}):
        """This class will act as a simple CLI utility
        
        Parameters
        ----------
        mandatory: list
            list of mandatory flags for utility
        optional: list, optional
            list of optional flags for utility
        types: dict, optional
            dictioanry of each flag with the type it should be 
            
        .. note:: -h or --help are reserved to rasis help requested error
    
        attributes
        ----------
        flags: list
            lais of all flags for utility
        args: dict
            dictionary of each flag with its values
            
        raises
        ------
        CLILibHelpRequestedError 
            if -h or --help are provided in list of flags
        
        """
        self.flags = mandatory + optional
        # print(self.flags)
        self.args = {}
        
        if '-h' in sys.argv[1:] or '--help' in sys.argv[1:]:
            raise CLILibHelpRequestedError
        
        for item in sys.argv[1:]:
            try:
                flag, value = item.split('=')
                self.args[flag] = value
            except ValueError:
                flag = item.split('=')[0]
                self.args[flag] = True

        # print(set(mandatory))
        # print(set(self.args.keys()))
        # print(set(self.flags))

        for flag in mandatory:
            if not flag in self.args.keys():
                raise CLILibMandatoryError(
                    "Invalid mandatory flags. Missing:", flag
                )

        for flag in self.args.keys():
            if not flag in self.flags:
                raise CLILibMandatoryError(
                    "Invalid flags:", flag
                )

        # if not set(mandatory) <= set(self.args.keys()) or \
        #     not set(self.args.keys()) <= set(self.flags):
        #     print (set(mandatory), set(self.args.keys()))
        #     raise CLILibMandatoryError("Invalid mandatory flags")
            
        
        if len(types) != 0:
            for flag in types:
                try:
                    self.args[flag] = types[flag]( self.args[flag] )
                except ValueError:
                    msg = 'Type of argument ' + flag + \
                        ' must be ' + str(types[flag]) + '.'
                    raise CLILibTypeError (msg)
                except KeyError:
                    pass
            
    def __repr__ (self):
        """
        """
        return str(self.args)
        
    def __getitem__ (self, key):
        """get opperator for utility, gets a flags value
        
        parameters
        ----------
        key: str
            the flag to get the value for
        
        returns
        -------
        the utilitys flags value
        """
        try:
            return self.args[key]
        except KeyError:
            if key in self.flags:
                return None
            else:
                raise KeyError(key)
        
        
        
        

if __name__ == "__main__":
    """ example utility """
    try:
        test = CLI(['--a','--b'],['--c'],
            types = {'--a':int, '--b': float, '--c':str})
        print (type(test['--a']), test['--a'])
        print (type(test['--b']), test['--b'])
        print (type(test['--c']), test['--c'])
    except  CLILibMandatoryError:
        print ("Valid flags are: --a, --b, and --c(optional)")
    except  CLILibTypeError as E:
        print (E)
        
    
    
