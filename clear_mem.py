import gc

def clear_mem(keep_list):
    '''
    Clear all the global variables to release memory - except those defined in keep_list
    :return:
    '''
    global_obj= [(key, value) for key, value in globals().items()]
    for key, value in global_obj:
        if callable(value) or value.__class__.__name__ == "module":
            continue

        if key not in keep_list:
            del globals()[key]
    gc.collect()
    print('Memory cleared for global vaiables.')



if __name__=='__main__':
    a=100
    b=1000
    print(a,b)

    print(key)
    clear_mem(['a'])
    print(a)
    print(b)