# UtilitairesAzureJava
Développement de petits utilitaires de discussion avec certains modules Azure tels que les events Hub, le stockage dans un blob, la requête dans une base PostgreSQL + PostGIS...

Il s'agit d'une utilisation très simple des technologies Azure pour :

Ecouter le message (une position xy) émit par une balise et envoyé à l'events Hub, pour ensuite reprojeter cette position de balise grâce à PostGreSQL + PostGIS embarqué dans Azure, stocker cette donnée dans un Block Blob Storage et dans le même temps envoyer ce message/cette position sur une file d'écoute au frontEnd pour réutiliser cette position xy transformée en Geojson, dans un webmapping temps réel
