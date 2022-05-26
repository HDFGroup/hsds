"""
  Perform simplified glob matching without using regular expressions
"""

def _wildcharmatch(item, pattern):
    # match item with pattern where pattern can contain any number of '?' chars
    i = 0 # index to item
    j = 0 # index to pattern
    matched_chars = 0  # number of chars in item that agree with pattern

    def nextItemChar():
        nonlocal i
        if i >= len(item):
            rv = None
        else:
            rv = item[i]
        i += 1
        return rv

    def nextPatternChar():
        nonlocal j
        if j >= len(pattern):
            rv = None
        else:
            rv = pattern[j]
        j += 1
        return rv

    char_range = None
    while True:
        q = nextItemChar()
        if q is None:   
            # no more chars 
            break

        p = nextPatternChar()
        if p is None:
            break
        if p == '[':
            min_ch = nextPatternChar()
            if min_ch is None:
                raise ValueError("expected at least one value in range pattern")
            p = nextPatternChar()
            if p == '-':
                # have a min and max range
                max_ch = nextPatternChar()
                if max_ch is None:
                    raise ValueError("missing max char in range pattern")
                p = nextPatternChar()
                if p != ']':
                    raise ValueError("expected closing bracket for range pattern")
            elif p == ']':
                # single value in range
                max_ch = min_ch
            else:
                raise ValueError("unexpected range pattern")
             
            char_range = (ord(min_ch), ord(max_ch))
            if char_range[1] < char_range[0]:
                raise ValueError("char range invalid")
        elif p == ']':
            # shouldn'tr have a close bracket unless in a range
            raise ValueError("unexpected closing bracket")
        elif p == '?':
            char_range = (0, 0xffff)
        else:
            char_range = (ord(p), ord(p))        
                 
        if ord(q) < char_range[0] or ord(q) > char_range[1]:
            # q did not fall in range
            break
        matched_chars += 1
        
    # end while
    if nextPatternChar() is None:
        # consumed the entire pattern, return number of chars matched 
        return matched_chars
    else:
        # pattern not matched return 0
        return 0


def globmatch(item, pattern):
    """
    Return True if item match pattern, where pattern uses simplified glob matching using wildcards:
    '*': match zero or more characters
    '?': match any one character
    '[n-m]': match any character range from n through m

    To avoid potential ddos attacks, at most one '*' can be supplied in pattern

    For a literal match, wrap the meta-characters in brackets. For example, '[?]' matches the character '?'.


    """
    if not item:
        return False
    if pattern == '*':
        return True # match anything
    # count '*' without backslash
    asterix_index = None
    for i in range(len(pattern)):
        if pattern[i] == '*' and (i==0 or pattern[i-1] != '['):
            if asterix_index is not None:
                raise ValueError("only one asterisk allowed in glob pattern")
            asterix_index = i  
    if asterix_index is None:
        if _wildcharmatch(item, pattern) == len(item):
            return True
        else:
            return False
    glob_left = pattern[:asterix_index]
    glob_right = pattern[(asterix_index + 1):]
    index = _wildcharmatch(item, glob_left)
    if glob_left and index == 0:
        return False
    item = item[index:]
    if not glob_right:
        return True  # '*' matches anything
    while item:
        if _wildcharmatch(item, glob_right) == len(item):
            return True
        item = item[1:]  # try with '*' globbing one more char
    
    return False

    
