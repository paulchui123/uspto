--
-- Create users and permissions for SHTF database configuration
--

-- First Flush all privileges from mysql
FLUSH PRIVILEGES;

-- Set the password function to use mysql_native_password
SET old_passwords = 0;

-- Create and set password for shtf@localhost
DROP USER IF EXISTS `uspto`@`localhost`;
CREATE USER IF NOT EXISTS `uspto`@`localhost` IDENTIFIED WITH mysql_native_password BY 'R5wM9N5qCEU3an#&rku8mxrVBuF@ur';

-- Grant privileges to all corresponding databases
GRANT ALL ON `uspto`.* TO `uspto`@`localhost`;

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL';

DROP SCHEMA IF EXISTS `uspto` ;
CREATE SCHEMA IF NOT EXISTS `uspto` DEFAULT CHARACTER SET utf8 ;
USE `uspto` ;

-- -----------------------------------------------------
-- Table `uspto`.`APPLICATION_PAIR`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`APPLICATION_PAIR` ;

CREATE TABLE IF NOT EXISTS `uspto`.`APPLICATION_PAIR` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `FileDate` DATE DEFAULT NULL,
  `AppType` VARCHAR(45) DEFAULT NULL,
  `ExaminerName` VARCHAR(100) DEFAULT NULL,
  `GroupArtUnit` VARCHAR(45) DEFAULT NULL,
  `ConfirmationNum` VARCHAR(45) DEFAULT NULL,
  `AttorneyDNum` VARCHAR(45) DEFAULT NULL,
  `ClassSubClass` VARCHAR(45) DEFAULT NULL,
  `InventorFName` VARCHAR(100) DEFAULT NULL,
  `CustomerNum` VARCHAR(45) DEFAULT NULL,
  `Status` VARCHAR(200) DEFAULT NULL,
  `StatusDate` DATE DEFAULT NULL,
  `Location` VARCHAR(100) DEFAULT NULL,
  `LocationDate` DATE DEFAULT NULL,
  `PubNoEarliest` VARCHAR(45) DEFAULT NULL,
  `PubDateEarliest` DATE DEFAULT NULL,
  `PatentNum` VARCHAR(45) DEFAULT NULL,
  `PatentIssueDate` DATE DEFAULT NULL,
  `TitleInvention` VARCHAR(500) DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`ApplicationID`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`APPLICATION`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`APPLICATION` ;

CREATE TABLE IF NOT EXISTS `uspto`.`APPLICATION` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `PublicationID` VARCHAR(20) DEFAULT NULL,
  `FileDate` DATE DEFAULT NULL,
  `Kind` VARCHAR(2) DEFAULT NULL,
  `USSeriesCode` VARCHAR(2) DEFAULT NULL,
  `AppType` VARCHAR(45) DEFAULT NULL,
  `PublishDate` DATE DEFAULT NULL,
  `Title` VARCHAR(2000) DEFAULT NULL,
  `Abstract` TEXT DEFAULT NULL,
  `ClaimsNum` INT DEFAULT NULL,
  `DrawingsNum` INT DEFAULT NULL,
  `FiguresNum` INT DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`ApplicationID`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`GRANT`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`GRANT` ;

CREATE TABLE IF NOT EXISTS `uspto`.`GRANT` (
  `GrantID` VARCHAR(20) NOT NULL,
  `IssueDate` DATE DEFAULT NULL,
  `Kind` VARCHAR(2) DEFAULT NULL,
  `USSeriesCode` VARCHAR(2) DEFAULT NULL,
  `Title` VARCHAR(2000) DEFAULT NULL,
  `Abstract` TEXT DEFAULT NULL,
  `Claims` TEXT DEFAULT NULL,
  `ClaimsNum` INT DEFAULT NULL,
  `DrawingsNum` INT DEFAULT NULL,
  `FiguresNum` INT DEFAULT NULL,
  `GrantLength` INT DEFAULT NULL,
  `ApplicationID` VARCHAR(20) DEFAULT NULL,
  `FileDate` DATE DEFAULT NULL,
  `AppType` VARCHAR(45) DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`GrantID`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`INTCLASS_A`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`INTCLASS_A` ;

CREATE TABLE IF NOT EXISTS `uspto`.`INTCLASS_A` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `Section` VARCHAR(15) DEFAULT NULL,
  `Class` VARCHAR(15) DEFAULT NULL,
  `SubClass` VARCHAR(15) DEFAULT NULL,
  `MainGroup` VARCHAR(15) DEFAULT NULL,
  `SubGroup` VARCHAR(15) DEFAULT NULL,
  `Malformed` BOOLEAN DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`ApplicationID`, `Position`, `FileName`))
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `uspto`.`CPCCLASS_A`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`CPCCLASS_A` ;

CREATE TABLE IF NOT EXISTS `uspto`.`CPCCLASS_A` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `Section` VARCHAR(15) DEFAULT NULL,
  `Class` VARCHAR(15) DEFAULT NULL,
  `SubClass` VARCHAR(15) DEFAULT NULL,
  `MainGroup` VARCHAR(15) DEFAULT NULL,
  `SubGroup` VARCHAR(15) DEFAULT NULL,
  `Malformed` BOOLEAN DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`ApplicationID`, `Position`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`USCLASS_A`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`USCLASS_A` ;

CREATE TABLE IF NOT EXISTS `uspto`.`USCLASS_A` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `Class` VARCHAR(5) DEFAULT NULL,
  `SubClass` VARCHAR(15) DEFAULT NULL,
  `Malformed` BOOLEAN DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`ApplicationID`, `Position`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`INVENTOR_A`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`INVENTOR_A` ;

CREATE TABLE IF NOT EXISTS `uspto`.`INVENTOR_A` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `FirstName` VARCHAR(100) DEFAULT NULL,
  `LastName` VARCHAR(100) DEFAULT NULL,
  `City` VARCHAR(100) DEFAULT NULL,
  `State` VARCHAR(100) DEFAULT NULL,
  `Country` VARCHAR(100) DEFAULT NULL,
  `Nationality` VARCHAR(100) DEFAULT NULL,
  `Residence` VARCHAR(300) DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`ApplicationID`, `Position`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`ATTORNEY`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`ATTORNEY` ;

CREATE TABLE IF NOT EXISTS `uspto`.`ATTORNEY` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `RegNo` VARCHAR(20) DEFAULT NULL,
  `FirstName` VARCHAR(45) DEFAULT NULL,
  `LastName` VARCHAR(45) DEFAULT NULL,
  `Phone` VARCHAR(45) DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`ApplicationID`, `Position`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`FOREIGNPRIORITY`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`FOREIGNPRIORITY_A` ;

CREATE TABLE IF NOT EXISTS `uspto`.`FOREIGNPRIORITY_A` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `DocumentID` VARCHAR(45) NOT NULL,
  `Position` INT NOT NULL,
  `Kind` VARCHAR(45) DEFAULT NULL,
  `Country` VARCHAR(100) DEFAULT NULL,
  `PriorityDate` DATE DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`ApplicationID`, `Position`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`TRANSACTION`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`TRANSACTION` ;

CREATE TABLE IF NOT EXISTS `uspto`.`TRANSACTION` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `Description` TINYTEXT DEFAULT NULL,
  `Date` DATE DEFAULT NULL,
  INDEX `fk_applicationid_transaction` (`ApplicationID` ASC) ,
  PRIMARY KEY (`ApplicationID`, `Position`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`CORRESPONDENCE`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`CORRESPONDENCE` ;

CREATE TABLE IF NOT EXISTS `uspto`.`CORRESPONDENCE` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `Name` VARCHAR(100) DEFAULT NULL,
  `Address` TINYTEXT DEFAULT NULL,
  `CustomerNum` VARCHAR(45) DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`ApplicationID`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`CONTINUITY_PARENT`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`CONTINUITY_PARENT` ;

CREATE TABLE IF NOT EXISTS `uspto`.`CONTINUITY_PARENT` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `Description` VARCHAR(100) DEFAULT NULL,
  `ParentNum` VARCHAR(45) DEFAULT NULL,
  `FileDate` DATE DEFAULT NULL,
  `ParentStatus` VARCHAR(45) DEFAULT NULL,
  `PatentNum` VARCHAR(45) DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  INDEX `FK_ApplicationID_CONTINUITY_PARENT` (`ApplicationID` ASC) ,
  PRIMARY KEY (`ApplicationID`, `Position`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`CONTINUITY_CHILD`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`CONTINUITY_CHILD` ;

CREATE TABLE IF NOT EXISTS `uspto`.`CONTINUITY_CHILD` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `Description` TINYTEXT DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  INDEX `FK_ApplicationID_CONTINUITY_CHILD` (`ApplicationID` ASC) ,
  PRIMARY KEY (`ApplicationID`, `Position`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`ADJUSTMENT`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`ADJUSTMENT` ;

CREATE TABLE IF NOT EXISTS `uspto`.`ADJUSTMENT` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `PriorAfter` TINYINT(1) DEFAULT NULL,
  `FileDate` DATE DEFAULT NULL,
  `IssueDate` DATE DEFAULT NULL,
  `PreIssuePetitions` VARCHAR(45) DEFAULT NULL,
  `PostIssuePetitions` VARCHAR(45) DEFAULT NULL,
  `USPTOAdjustDays` VARCHAR(45) DEFAULT NULL,
  `USPTODelayDays` VARCHAR(45) DEFAULT NULL,
  `ThreeYears` VARCHAR(45) DEFAULT NULL,
  `APPLDelayDays` VARCHAR(45) DEFAULT NULL,
  `TotalTermAdjustDays` VARCHAR(45) DEFAULT NULL,
  `ADelays` VARCHAR(45) DEFAULT NULL,
  `BDelays` VARCHAR(45) DEFAULT NULL,
  `CDelays` VARCHAR(45) DEFAULT NULL,
  `OverlapDays` VARCHAR(45) DEFAULT NULL,
  `NonOverlapDelays` VARCHAR(45) DEFAULT NULL,
  `PTOManualAdjust` VARCHAR(45) DEFAULT NULL,
  `FileName` INT DEFAULT NULL
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`ADJUSTMENTDESC`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`ADJUSTMENTDESC` ;

CREATE TABLE IF NOT EXISTS `uspto`.`ADJUSTMENTDESC` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `PriorAfter` TINYINT(1) DEFAULT NULL,
  `Number` INT DEFAULT NULL,
  `Date` DATE DEFAULT NULL,
  `ContentDesc` TINYTEXT DEFAULT NULL,
  `PTODays` VARCHAR(45) DEFAULT NULL,
  `APPLDays` VARCHAR(45) DEFAULT NULL,
  `Start` VARCHAR(45) DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`ApplicationID`, `Position`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`EXTENSION`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`EXTENSION` ;

CREATE TABLE IF NOT EXISTS `uspto`.`EXTENSION` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `FileDate` DATE DEFAULT NULL,
  `USPTOAdjustDays` INT DEFAULT NULL,
  `USPTODelays` INT DEFAULT NULL,
  `CorrectDelays` INT DEFAULT NULL,
  `TotalExtensionDays` INT DEFAULT NULL,
  `FileName` INT DEFAULT NULL
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`EXTENSIONDESC`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`EXTENSIONDESC` ;

CREATE TABLE IF NOT EXISTS `uspto`.`EXTENSIONDESC` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `Date` DATE DEFAULT NULL,
  `Description` TINYTEXT DEFAULT NULL,
  `PTODays` VARCHAR(45) DEFAULT NULL,
  `APPLDays` VARCHAR(45) DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`ApplicationID`, `Position`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`AGENT_A`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`AGENT_A` ;

CREATE TABLE IF NOT EXISTS `uspto`.`AGENT_A` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `OrgName` VARCHAR(300) DEFAULT NULL,
  `LastName` VARCHAR(100) DEFAULT NULL,
  `FirstName` VARCHAR(100) DEFAULT NULL,
  `Country` VARCHAR(100) DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`ApplicationID`, `Position`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`ASSIGNEE_A`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`ASSIGNEE_A` ;

CREATE TABLE IF NOT EXISTS `uspto`.`ASSIGNEE_A` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `OrgName` VARCHAR(300) DEFAULT NULL,
  `Role` VARCHAR(45) DEFAULT NULL,
  `City` VARCHAR(100) DEFAULT NULL,
  `State` VARCHAR(100) DEFAULT NULL,
  `Country` VARCHAR(100) DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`ApplicationID`, `Position`, `FileName`))
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `uspto`.`APPLICANT_A`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`APPLICANT_A` ;

CREATE TABLE IF NOT EXISTS `uspto`.`APPLICANT_A` (
  `ApplicationID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `OrgName` VARCHAR(300) DEFAULT NULL,
  `FirstName` VARCHAR(100) DEFAULT NULL,
  `LastName` VARCHAR(100) DEFAULT NULL,
  `City` VARCHAR(100) DEFAULT NULL,
  `State` VARCHAR(100) DEFAULT NULL,
  `Country` VARCHAR(100) DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`ApplicationID`, `Position`, `FileName`))
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `uspto`.`USCLASSIFICATION`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`USCLASSIFICATION` ;

CREATE TABLE IF NOT EXISTS `uspto`.`USCLASSIFICATION` (
  `ClassID` INT(9) NOT NULL AUTO_INCREMENT ,
  `Class` VARCHAR(3) NULL,
  `SubClass` VARCHAR(6) DEFAULT NULL,
  `Indent` VARCHAR(2) DEFAULT  NULL,
  `SubClassSqsNum` VARCHAR(4) DEFAULT NULL,
  `NextHigherSub` VARCHAR(6) DEFAULT NULL,
  `Title` VARCHAR(200) DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`ClassID`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`INTCLASS_G`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`INTCLASS_G` ;

CREATE TABLE IF NOT EXISTS `uspto`.`INTCLASS_G` (
  `GrantID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `Section` VARCHAR(15) DEFAULT NULL,
  `Class` VARCHAR(15) DEFAULT NULL,
  `SubClass` VARCHAR(15) DEFAULT NULL,
  `MainGroup` VARCHAR(15) DEFAULT NULL,
  `SubGroup` VARCHAR(15) DEFAULT NULL,
  `Malformed` BOOLEAN DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`GrantID`, `Position`, `FileName`))
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `uspto`.`CPCCLASS_G`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`CPCCLASS_G` ;

CREATE TABLE IF NOT EXISTS `uspto`.`CPCCLASS_G` (
  `GrantID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `Section` VARCHAR(15) DEFAULT NULL,
  `Class` VARCHAR(15) DEFAULT NULL,
  `SubClass` VARCHAR(15) DEFAULT NULL,
  `MainGroup` VARCHAR(15) DEFAULT NULL,
  `SubGroup` VARCHAR(15) DEFAULT NULL,
  `Malformed` BOOLEAN DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`GrantID`, `Position`, `FileName`))
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `uspto`.`NONPATCIT_G`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`NONPATCIT_G` ;

CREATE TABLE IF NOT EXISTS `uspto`.`NONPATCIT_G` (
  `GrantID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `Citation` TEXT DEFAULT NULL,
  `Category` BOOLEAN DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  INDEX `fk_ApplicationID_APPLICATION_NPCITATION_A` (`GrantID` ASC) ,
  PRIMARY KEY (`GrantID`, `Position`, `FileName`))
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `uspto`.`APPLICANT_G`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`APPLICANT_G` ;

CREATE TABLE IF NOT EXISTS `uspto`.`APPLICANT_G` (
  `GrantID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `OrgName` VARCHAR(300) DEFAULT NULL,
  `FirstName` VARCHAR(100) DEFAULT NULL,
  `LastName` VARCHAR(100) DEFAULT NULL,
  `City` VARCHAR(100) DEFAULT NULL,
  `State` VARCHAR(100) DEFAULT NULL,
  `Country` VARCHAR(100) DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`GrantID`, `Position`, `FileName`))
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `uspto`.`INVENTOR_G`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`INVENTOR_G` ;

CREATE TABLE IF NOT EXISTS `uspto`.`INVENTOR_G` (
  `GrantID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `FirstName` VARCHAR(100) DEFAULT NULL,
  `LastName` VARCHAR(100) DEFAULT NULL,
  `City` VARCHAR(100) DEFAULT NULL,
  `State` VARCHAR(100) DEFAULT NULL,
  `Country` VARCHAR(100) DEFAULT NULL,
  `Nationality` VARCHAR(100) DEFAULT NULL,
  `Residence` VARCHAR(300) DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`GrantID`, `Position`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`USCLASS_G`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`USCLASS_G` ;

CREATE TABLE IF NOT EXISTS `uspto`.`USCLASS_G` (
  `GrantID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `Class` VARCHAR(5) DEFAULT NULL,
  `SubClass` VARCHAR(15) DEFAULT NULL,
  `Malformed` BOOLEAN DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`GrantID`, `Position`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`AGENT_G`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`AGENT_G` ;

CREATE TABLE IF NOT EXISTS `uspto`.`AGENT_G` (
  `GrantID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `OrgName` VARCHAR(300) DEFAULT NULL,
  `LastName` VARCHAR(100) DEFAULT NULL,
  `FirstName` VARCHAR(100) DEFAULT NULL,
  `Country` VARCHAR(100) DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`GrantID`, `Position`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`ASSIGNEE_G`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`ASSIGNEE_G` ;

CREATE TABLE IF NOT EXISTS `uspto`.`ASSIGNEE_G` (
  `GrantID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `OrgName` VARCHAR(500) DEFAULT NULL,
  `Role` VARCHAR(45) DEFAULT NULL,
  `City` VARCHAR(100) DEFAULT NULL,
  `State` VARCHAR(100) DEFAULT NULL,
  `Country` VARCHAR(100) DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`GrantID`, `Position`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`EXAMINER_G`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`EXAMINER_G` ;

CREATE TABLE IF NOT EXISTS `uspto`.`EXAMINER_G` (
  `GrantID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `LastName` VARCHAR(50) DEFAULT NULL,
  `FirstName` VARCHAR(50) DEFAULT NULL,
  `Department` VARCHAR(100) DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`GrantID`, `Position`, `FileName`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `uspto`.`GRACIT_G`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`GRACIT_G` ;

CREATE TABLE IF NOT EXISTS `uspto`.`GRACIT_G` (
  `GrantID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `CitedID` VARCHAR(20) DEFAULT NULL,
  `Kind` VARCHAR(10) DEFAULT NULL,
  `Name` VARCHAR(100) DEFAULT NULL,
  `Date` DATE DEFAULT NULL,
  `Country` VARCHAR(100) DEFAULT NULL,
  `Category` BOOLEAN DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`GrantID`, `Position`, `FileName`))
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `uspto`.`FORPATCIT_G`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`FORPATCIT_G` ;

CREATE TABLE IF NOT EXISTS `uspto`.`FORPATCIT_G` (
  `GrantID` VARCHAR(20) NOT NULL,
  `Position` INT NOT NULL,
  `CitedID` VARCHAR(25) DEFAULT NULL,
  `Kind` VARCHAR(10) DEFAULT NULL,
  `Name` VARCHAR(100) DEFAULT NULL,
  `Date` DATE DEFAULT NULL,
  `Country` VARCHAR(100) DEFAULT NULL,
  `Category` BOOLEAN DEFAULT NULL,
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`GrantID`, `Position`, `FileName`))
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `uspto`.`STARTED_FILES`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `uspto`.`STARTED_FILES` ;

CREATE TABLE IF NOT EXISTS `uspto`.`STARTED_FILES` (
  `FileName` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`FileName`))
ENGINE = InnoDB;

SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
