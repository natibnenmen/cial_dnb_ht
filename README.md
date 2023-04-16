# Data Extractor

## Usage
### Running natively on the host
#### 1. Clone  the following git repo:
    git@github.com:natibnenmen/cial_dnb_ht.git

#### 2. On the root directory, run:
    >python3 process.py <zip file>

When the <zip file> is the input zip file which contains the 'debts.txt' input file.

### Running a Docker container
#### Run the following command:
    docker run --rm -v <host dir>:<container dir> --name <container name> natibenmen/dnb-data-extractor:1.0 python3 ./process.py <container dir>/<input zip file>

Where:
    <host dir> is a directory on the host which contains the the <input zip file>