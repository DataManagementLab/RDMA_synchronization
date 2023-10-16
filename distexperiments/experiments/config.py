from distexprunner import ServerList, Server


SERVER_PORT = 20005


server_list = ServerList(
    # fill in
    Server('node08', 'c08.lab.dm.informatik.tu-darmstadt.de', SERVER_PORT, ibIp='172.18.94.80', sibIP='172.18.94.81', ssdPath="/dev/md0"),
    Server('node07', 'c07.lab.dm.informatik.tu-darmstadt.de', SERVER_PORT, ibIp='172.18.94.70', sibIP='172.18.94.71', ssdPath="/dev/md0 "),
    Server('node06', 'c06.lab.dm.informatik.tu-darmstadt.de', SERVER_PORT, ibIp='172.18.94.60', sibIP='172.18.94.61', ssdPath="/dev/md0"),
    
    Server('node04', 'c04.lab.dm.informatik.tu-darmstadt.de', SERVER_PORT, ibIp='172.18.94.40', sibIP='172.18.94.41', ssdPath="/dev/md127"),
    Server('node05', 'c05.lab.dm.informatik.tu-darmstadt.de', SERVER_PORT, ibIp='172.18.94.50', sibIP='172.18.94.51', ssdPath="/dev/md0"),
    Server('node02', 'c02.lab.dm.informatik.tu-darmstadt.de', SERVER_PORT, ibIp='172.18.94.20', sibIP='172.18.94.21', ssdPath="/dev/md0"),
    Server('node01', 'c01.lab.dm.informatik.tu-darmstadt.de', SERVER_PORT, ibIp='172.18.94.10', sibIP='172.18.94.11', ssdPath="/dev/md0"),
)
