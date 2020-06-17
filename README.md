# UtilitairesAzureJava
Développement de petits utilitaires de discussion avec certains modules Azure tels que les events Hub, le stockage dans un blob, la requête dans une base PostgreSQL + PostGIS...

Il s'agit d'une utilisation très simple des technologies Azure pour :

Ecouter le message (une position xy) émit par une balise et envoyé à l'events Hub, pour ensuite reprojeter cette position de balise grâce à PostGreSQL + PostGIS embarqué dans Azure, et enfin stocker cette donnée dans un Block Blob Storage
