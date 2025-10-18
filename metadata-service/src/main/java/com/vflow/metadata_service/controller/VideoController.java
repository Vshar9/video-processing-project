package com.vflow.metadata_service.controller;

import com.vflow.metadata_service.model.Video;
import com.vflow.metadata_service.repository.VideoRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;

@RestController
@RequestMapping("/videos")
public class VideoController{
    @Autowired
    private VideoRepository videoRepository;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @PostMapping
    public ResponseEntity<Video> createVideoMetadata(@RequestBody Video video){
        video.setStatus("PENDING");
        Video savedVideo = videoRepository.save(video);
        return new ResponseEntity<>(savedVideo,HttpStatus.CREATED);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Video> getVideoById(@PathVariable Long id){
        Optional<Video> videoData = videoRepository.findById(id);
        return videoData.map(video->new ResponseEntity<>(video,HttpStatus.OK))
            .orElseGet(()->new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    @PutMapping("/{id}")
    public ResponseEntity<Video> updateVideo(@PathVariable Long id, @RequestBody Video videoDetails){
        Optional<Video> videoData = videoRepository.findById(id);
        if(videoData.isPresent()){
            Video existingVideo = videoData.get();
            boolean statusChangedToUploaded = false;
            if(videoDetails.getStatus()!=null){
                if("UPLOADED".equalsIgnoreCase(videoDetails.getStatus()) && !"UPLOADED".equalsIgnoreCase(existingVideo.getStatus())){
                    statusChangedToUploaded = true;
                }
                existingVideo.setStatus(videoDetails.getStatus());
            }
            if(videoDetails.getVideoUrl()!=null){
                existingVideo.setVideoUrl(videoDetails.getVideoUrl());
            }
            if(videoDetails.getOutputUrl()!=null){
                existingVideo.setOutputUrl(videoDetails.getOutputUrl());
            }
            Video updatedVideo = videoRepository.save(existingVideo);

            if(statusChangedToUploaded){
                System.out.println("Publishing video uploaded event for video ID: "+updatedVideo.getId());
                try{
                    String routingKey = "video.uploaded."+updatedVideo.getProcessingType();
                    String message = objectMapper.writeValueAsString(updatedVideo);
                    rabbitTemplate.convertAndSend("video_events",routingKey,message);
                    System.out.println("Event published successfully with routing key: "+routingKey);
                }catch(Exception e){
                    System.err.println("Failed to serialize video object or publish message: "+ e.getMessage());
                }
            }
            
            return new ResponseEntity<>(updatedVideo,HttpStatus.OK);
        }else{
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }
}