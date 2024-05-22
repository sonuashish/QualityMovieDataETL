# QualityMovieDataETL



#Challenges Faced

While Adding the Redshift data to Quicksight, it was diffucult for me to connect the redsift  database with QuickSIght.
How I overcome it?
First, I created an Endpoint with QuickSight services to validate the connection. But it failed.
Then I made the cluste access to public, it made the Cluste VPC Public from Private, which allowed the Quicksight to detect the Public VPC while creating the connection.
Still I was not able to get the services connected. After that I checked on the officical documentation and learned that I had to make the Inbound rule set to all in the security group.
Hence, I checked on VPC service, under that I configured the Security group's Inbound rule set to public IP for my cluster.
This helped me connect my data with the Quicksight.
