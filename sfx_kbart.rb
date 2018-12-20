#!/usr/bin/env ruby
# -*- coding: UTF-8 -*-

# loop through files in weekly kbart extract in $SFXLCL41/dbs/scratch
# extract target name from filename and add to combined output file
# folder path is `./princeton`
# pmg
# from 20181116

require 'csv'
require 'date'
require 'fileutils'
require 'logger'
require 'net/ftp'
require 'net/scp'
require 'net/ssh'
require 'yaml'
require 'zip'
#require 'zlib'

#$conf = YAML.load_file('sfx_kbart.yml')
$kbart_server = ENV['READCUBE_HOST'] #$conf['host'] 
$kbart_user = ENV['READCUBE_USER']#$conf['user'] 
$kbart_pwd = ENV['READCUBE_PASSWORD'] #$conf['pwd']
$sfx_server = ENV['SFX_HOST']  #$conf['sfx_host']
$sfx_user = ENV['SFX_USER'] #$conf['sfx_user']
$sfx_pwd = ENV['SFX_PASSWORD']  #$conf['sfx_pwd']
$local_dir = './scratch'
$out_dir = './out'
$logger = Logger.new('logfile.log','weekly')

# TODO: error handling

def main()
	$logger.info '=' * 25
	$logger.info 'program started' 
	scp_from_sfx()
	local_zip_dir = './scratch/export_kbart_*.zip'
	temp_dir = './temp'
	filename = ''
	f = Dir.glob(local_zip_dir).max_by {|f| File.mtime(f)}
	puts f
		filename = File.basename(f)
		filename = filename[13..26]
		extract_zip(f,temp_dir)
	filename = 'EXPORT_PORTFOLIOS_PRINCETON_SFX_'+filename+'.tsv'
	combine_files(temp_dir,filename)
	cleanup(temp_dir)
	ftp_files(filename)
	$logger.info('all done')
	$logger.info '=' * 25
end


def extract_zip(z,temp_dir)
	'''
	unzip the weekly extract
	'''
	Zip::File.open(z) do |zipfile|
		zipfile.each do |file|
			file_path = File.join(temp_dir,file.name)
			FileUtils.mkdir_p(File.dirname(file_path))
			zipfile.extract(file,file_path) unless File.exist?(file_path)
		end
	end
	$logger.info('extracted files')
end


def combine_files(indir,outfile)
	'''
	print header row
	'''
	$logger.info('combining files')
	CSV.open(outfile, "wb+",{:col_sep => "\t"}) do |csv| 
		csv << ['publication_title','print_identifier','online_identifier','date_first_issue_online','num_first_vol_online','num_first_issue_online','date_last_issue_online','num_last_vol_online','num_last_issue_online','title_url','first_author','title_id','embargo_info','coverage_depth','coverage_notes','publisher_name','access_type','publication_type','interface_name']
	end
	Dir.glob(indir+'/*.txt') do |file|
		next if file == '.' or file == '..'
		
		CSV.read(file, "r", { :col_sep => "\t" , :headers => true , :quote_char => "\x00"}).each do |row|
			filename = File.basename(file)
			filename = filename[0..-20] # filename is the name of the target, minus the date and .txt
			
			CSV.open(outfile, "ab+", { :col_sep => "\t"}) do |csv|
				row << filename # add the filename to each row
				csv << row # write the row to the outfile
			end
		end
	end
	$logger.info('files combined')
end


def cleanup(extracted_files)
	'''
	just cleanup the temp dir of extracted files
	'''
	FileUtils.rm_rf(extracted_files)
	$logger.info('cleaned up')
end


def compress_file(combo_file)
	'''
	zip up the file
	'''
	# TODO: check, is this needed?
	combo_file_noext = File.basename(combo_file,'.tsv')
	save_file_path = combo_file_noext + '.zip'
	Zip::File.open(save_file_path, Zip::File::CREATE) do |zipfile|
		zipfile.add('test',combo_file)
	end
end


def ftp_files(localfile)
	'''
	ftp resulting files to access anywhere server
	'''
	Net::FTP.open($kbart_server,$kbart_user,$kbart_pwd) do |ftp|
	ftp.chdir('/princeton')
	ftp.putbinaryfile(localfile)
	$logger.info('%s ftped over to %s' % [localfile,$kbart_server])
	end
end


def scp_from_sfx()
	'''
	Gets the most recent export_kbart_*.zip file
	'''
	datum = ''

	Net::SSH.start($sfx_server, $sfx_user, :password => $sfx_pwd) do |ssh|
		this_monday = Date.parse('Monday') # head -1 doesn't work reliably for some reason so adding this as well
		this_monday = this_monday.strftime('%Y%m%d')
		ssh.exec!("find /exlibris/sfx_ver/sfx4_1/sfxlcl41/dbs/scratch/ -name '*"+this_monday+"*.zip' | head -1") do |channel, stream, data|
			datum = data
		end
		Net::SCP.start($sfx_server, $sfx_user, :password => $sfx_pwd) do |scp|
			$logger.info('scp started')
			scp.download! datum.strip, $local_dir
			$logger.info('%s downloaded to %s' % [datum.strip,$local_dir])
		end
	end
end


if __FILE__ == $0
	main()
end

