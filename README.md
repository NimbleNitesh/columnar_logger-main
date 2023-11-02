# Software Engineer: Take-Home Project

## Set up Instructions:
* Download and unzip the file.
* Open Terminal and write `cargo run --release` to run the application. Make sure you have rust compiler and cargo package manager installed in your system.
* Enter the location of input file as well as the output folder location.

## Summary Document
### Approach
A simple brute force idea can be to parse the json line by line and generate the `key-value` pairs and then create/open the file named `key` and append `value` in it. This approach was highly time taking. Even for 128 MB file, the time taken was close to 5 minutes.

Here my key observation is that each **write operations are independent** of each other. Hence I modified the approach creating 8 threads for the write operation. Also, instead of typical Write command, I use `BufWriter` module of Rust which is much more efficient to make small and repeated write calls to the same file.


### Performance Table:
| File Size | Time taken(sec) | Memory |
|-----------|-----------------|--------|
| 128 MB | **3.00**  | 300 MB       |
| 256 MB | **6.00**  | 600 MB       |
| 512 MB | **12.00** | 1100 MB      |

## Testable CLI 
Just run the program. The CLI interface is built to take the path to a log file and generate the output at the desired location.

## Demo Video
Link: <a href='https://drive.google.com/file/d/17DED5Oew2FswI4TyryQ_fnCFZUtLKkrd/view?usp=sharing'>Click Here</a>

## Limitations
I haven't tested the code for large files of range *4 GB to 48 GB*. But I believe we need to augment the code a little more for them. For example we would need to read the code in batches of size of around **1 GB**. Directly loading a huge file into the memory will most probably trigger the OS to kill the program.