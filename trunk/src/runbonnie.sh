mkdir $2/bonnie0
mkdir $2/bonnie1
mkdir $2/bonnie2
mkdir $2/bonnie3
bonnie++ -p4
bonnie++ -d $2/bonnie0 -y -r 1024 -n 0:0:0:0 -m "$1" >>~/bonnie0.csv &
bonnie++ -d $2/bonnie1 -y -r 1024 -n 0:0:0:0 -m "$1" >>~/bonnie1.csv &
bonnie++ -d $2/bonnie2 -y -r 1024 -n 0:0:0:0 -m "$1" >>~/bonnie2.csv &
bonnie++ -d $2/bonnie3 -y -r 1024 -n 0:0:0:0 -m "$1" >>~/bonnie3.csv &