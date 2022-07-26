CREATE TABLE `performance_per_hour` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `registration_number` varchar(100) NOT NULL,
  `first_name` varchar(100) NOT NULL,
  `last_name` varchar(100) NOT NULL,
  `performance` float NOT NULL,
  `cur_day` varchar(100) NOT NULL,
  `cur_time` varchar(100) NOT NULL,
  PRIMARY KEY (`id`)
);