CREATE DATABASE IF NOT EXISTS gmca_accounts;
USE gmca_accounts;

-- 1. Create 'user' table
CREATE TABLE IF NOT EXISTS `user` (
    `badge_number` INT PRIMARY KEY,
    `username` VARCHAR(50) NOT NULL UNIQUE,
    `full_name` VARCHAR(100)
);

-- 2. Create 'group' table
CREATE TABLE IF NOT EXISTS `group` (
    `group_name` VARCHAR(50) PRIMARY KEY,
    `esaf_number` INT,
    `pi_badge` INT,
    `beamline` VARCHAR(10),
    `esaf_collect_start` DATETIME,
    `esaf_collect_end` DATETIME,
    `group_enable` INT DEFAULT 1
);

-- 3. Create 'user_group' table
CREATE TABLE IF NOT EXISTS `user_group` (
    `badge_number` INT,
    `group_name` VARCHAR(50),
    PRIMARY KEY (`badge_number`, `group_name`),
    FOREIGN KEY (`badge_number`) REFERENCES `user`(`badge_number`),
    FOREIGN KEY (`group_name`) REFERENCES `group`(`group_name`)
);

-- 4. Populate with mock data
-- Create a staff user 'dhs'
INSERT IGNORE INTO `user` (`badge_number`, `username`, `full_name`) VALUES (1, 'dhs', 'DHS Staff');
INSERT IGNORE INTO `group` (`group_name`) VALUES ('staff');
INSERT IGNORE INTO `user_group` (`badge_number`, `group_name`) VALUES (1, 'staff');

-- Create a mock user 'qxu'
INSERT IGNORE INTO `user` (`badge_number`, `username`, `full_name`) VALUES (2, 'qxu', 'Q Xu');
INSERT IGNORE INTO `group` (`group_name`, `esaf_number`, `beamline`) VALUES ('esaf12345', 12345, '23b');
INSERT IGNORE INTO `user_group` (`badge_number`, `group_name`) VALUES (2, 'esaf12345');

-- Create a mock 'qxu' group just in case primary group logic uses it
INSERT IGNORE INTO `group` (`group_name`) VALUES ('qxu');
INSERT IGNORE INTO `user_group` (`badge_number`, `group_name`) VALUES (2, 'qxu');
