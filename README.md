# ScrapFipeVeiculos

This Python script scrapes historical data on vehicle prices from https://veiculos.fipe.org.br/ and can be used to collect data on cars, motorcycles, and trucks. Note that it was written to fit my specific use case (it reads and writes to my MySQL database) and cannot be easily generalized. However, the functions provided can be useful as a starting point for similar projects.

## Example usage:

To use the script, open your terminal and run the following command:

`python ScrapFipeVeiculos.py <num_workers> <vehicle_type> <user> <password>`

`<num_workers>`: The number of parallel processing threads to use.

`<vehicle_type>`: The type of vehicle to scrape. Enter 1 for cars, 2 for motorcycles, or 3 for trucks.

`<user>` and `<password>`: Credentials to access MySQL database.
