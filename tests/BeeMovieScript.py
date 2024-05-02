from gladier import ProvenanceBaseTool, generate_flow_definition

def GetScript(**data):
    """Grab the Bee Movie Script"""
    import requests
    import shutil

    # URL of the Bee Movie script
    url = "https://courses.cs.washington.edu/courses/cse163/20wi/files/lectures/L04/bee-movie.txt"
    response = requests.get(url)
    if response.status_code == 200:
      with open('input/test.txt', 'w') as file:
        file.write(response.text)
      shutil.move('input/test.txt', 'output/test.txt')
    else:
      print("Failed to download the script.")
    

@generate_flow_definition
class GetBeeMovieScript(ProvenanceBaseTool):
    """Get the Bee Movie Script"""
    compute_functions = [GetScript]

